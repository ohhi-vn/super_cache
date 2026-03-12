defmodule SuperCache.ClusterCase do
  @moduledoc """
  Shared ExUnit case template for cluster integration tests.

  ## Usage

      use SuperCache.ClusterCase

  Injects into the using module:
  - `@moduletag :cluster` and `timeout: 120_000`
  - Aliases: `Manager`, `NodeMonitor`, `Cache` (`SuperCache.Distributed`)
  - All helper functions below, via `import`
  - A per-test `setup` callback that reads fresh peer refs from the agent
    and calls `safe_delete_all/0`

  Each test file must define its own `setup_all` and call
  `setup_cluster!/2` with a pair of **unique** short-name atoms so that
  files running back-to-back do not race on the same Erlang node-name.

  ## Example

      setup_all do
        setup_cluster!(:my_peer1, :my_peer2)
      end
  """

  import ExUnit.Assertions

  alias SuperCache.Cluster.{Manager, NodeMonitor}

  # Required here (not just in __using__) because safe_delete_all/0 calls
  # Logger.debug/1, which is a macro.  The require in __using__ only covers
  # test modules that use this case template; it does not apply to ClusterCase
  # itself.
  require Logger

  # Base cache opts shared by every test file.
  # Node-source keys (:nodes / :nodes_mfa / :refresh_ms) are intentionally
  # omitted so NodeMonitor starts in legacy :all mode; individual tests that
  # need a restricted managed set call restart_node_monitor/1 explicitly.
  @cache_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    num_partition:      8,
    table_type:         :set
  ]

  @doc "Return the base cache opts used by every cluster test file."
  def cache_opts, do: @cache_opts

  # ── Cluster lifecycle ───────────────────────────────────────────────────────

  @doc """
  Start a three-node cluster (test node + two peers) and return a context
  map suitable for returning from `setup_all`.

  Pass unique short-name atoms for the two peer nodes so that different
  test files do not conflict on the same Erlang node-name atom.

  An `on_exit` callback is registered automatically to stop both peers and
  call `Bootstrap.stop/0` after the test suite finishes.
  """
  @spec setup_cluster!(atom, atom) :: map
  def setup_cluster!(peer1_name, peer2_name) do
    if node() == :nonode@nohost do
      raise "Cluster tests require a distributed node. Run with: mix test.cluster"
    end

    {:ok, _} = Application.ensure_all_started(:super_cache)

    if SuperCache.started?() do
      SuperCache.Cluster.Bootstrap.stop()
      Process.sleep(100)
    end

    SuperCache.Cluster.Bootstrap.start!(@cache_opts)

    {peer1, node1} = start_peer(peer1_name)
    {peer2, node2} = start_peer(peer2_name)

    true = Node.connect(node1)
    true = Node.connect(node2)
    true = :erpc.call(node1, Node, :connect, [node2], 5_000)

    assert wait_until(
             fn ->
               try do
                 length(Manager.live_nodes()) == 3
               catch
                 _, _ -> false
               end
             end,
             10_000
           ),
           "Local Manager did not reach 3 live nodes within 10 s"

    # Switch all three NodeMonitors to static lists now that membership is
    # complete.  Switching early with a partial list causes NodeMonitor to
    # silently filter subsequent :nodeup events for nodes not yet managed.
    restart_node_monitor(nodes: [node1, node2])
    :erpc.call(node1, NodeMonitor, :reconfigure, [[nodes: [node(), node2]]], 5_000)
    :erpc.call(node2, NodeMonitor, :reconfigure, [[nodes: [node(), node1]]], 5_000)

    assert wait_cluster_stable(3),
           "Cluster views did not converge to 3 nodes. " <>
             "Live nodes: #{inspect(Manager.live_nodes())}"

    IO.puts("Cluster stable: #{inspect(Manager.live_nodes())}")

    {:ok, agent} =
      Agent.start_link(fn ->
        %{peer1: peer1, node1: node1, peer2: peer2, node2: node2}
      end)

    # Register cleanup so callers don't have to repeat it in on_exit.
    ExUnit.Callbacks.on_exit(fn ->
      state = Agent.get(agent, & &1)
      stop_peer(state.peer1)
      stop_peer(state.peer2)

      try do
        SuperCache.Cluster.Bootstrap.stop()
      catch
        :exit, _ -> :ok
      end
    end)

    %{agent: agent, node1: node1, node2: node2, peer1: peer1, peer2: peer2}
  end

  # ── Peer helpers ────────────────────────────────────────────────────────────

  @doc """
  Start a peer node, load the SuperCache code path and OTP app, and call
  `Bootstrap.start!/1` with the base cache opts.

  Uses `-connect_all false` so the test controls all Erlang-level
  connections explicitly.
  """
  @spec start_peer(atom) :: {pid, node}
  def start_peer(name) do
    cookie    = :erlang.get_cookie()
    code_path = :code.get_path()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie", :erlang.atom_to_list(cookie),
          ~c"-connect_all", ~c"false"
        ]
      })

    # Give the remote VM a moment to stabilise before loading code.
    Process.sleep(1_000)

    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])

    {peer, node}
  end

  @doc """
  Start a plain Erlang peer with no SuperCache OTP app.

  Used to verify that NodeMonitor ignores Erlang nodes that are not part
  of the cache cluster.
  """
  @spec start_plain_peer(atom) :: {pid, node}
  def start_plain_peer(name) do
    cookie = :erlang.get_cookie()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie", :erlang.atom_to_list(cookie),
          ~c"-connect_all", ~c"false"
        ]
      })

    {peer, node}
  end

  @doc "Stop a peer, swallowing any exit if it is already dead."
  @spec stop_peer(pid) :: :ok
  def stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  @doc """
  Start a fresh peer under `name`, reconnect it to all existing live nodes,
  notify `Manager`, and wait for full cluster stability.

  Raises if the restarted node does not appear in `Manager.live_nodes/0`
  within 15 s.
  """
  @spec restart_peer(atom | node) :: {pid, node}
  def restart_peer(name) do
    {peer, node} = start_peer(name)

    true = Node.connect(node)

    # With -connect_all false, reconnect the new peer to every existing live
    # member so :nodeup fires on all sides and every Manager gains full
    # membership.
    Manager.live_nodes()
    |> List.delete(node())
    |> Enum.each(fn other ->
      try do
        :erpc.call(node, Node, :connect, [other], 5_000)
      catch
        _, _ -> :ok
      end
    end)

    # Explicit node_up in case the :nodeup kernel event raced ahead of
    # Bootstrap.start!/1 completing on the new peer.
    Manager.node_up(node)

    assert wait_until(fn -> node in Manager.live_nodes() end, 15_000),
           "Restarted node #{inspect(node)} did not join Manager.live_nodes"

    wait_cluster_stable()
    {peer, node}
  end

  @doc "Reconfigure NodeMonitor without restarting the OTP supervisor."
  @spec restart_node_monitor(keyword) :: :ok
  def restart_node_monitor(opts), do: NodeMonitor.reconfigure(opts)

  # ── Read / assert helpers ───────────────────────────────────────────────────

  @doc """
  Read records for `key` from `target_node`'s local ETS (bypasses routing).
  `partition_value` is used to derive the partition order index.
  """
  @spec node_read(node, term, term) :: [tuple]
  def node_read(target_node, key, partition_value) do
    order = SuperCache.Partition.get_partition_order(partition_value)
    :erpc.call(target_node, SuperCache.Cluster.Router, :local_read, [order, :get, key], 8_000)
  end

  @doc """
  Poll `target_node`'s local ETS every 50 ms until the stored records for
  `key` equal `expected`, or `timeout` ms elapses (default 2 s).

  Calls `flunk/1` on timeout so the enclosing test fails with a clear message.
  """
  @spec assert_node_has(node, term, term, [tuple], non_neg_integer) :: :ok
  def assert_node_has(target_node, key, partition_value, expected, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_node_has(target_node, key, partition_value, expected, deadline)
  end

  defp do_assert_node_has(target_node, key, partition_value, expected, deadline) do
    got = node_read(target_node, key, partition_value)

    cond do
      got == expected ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "Node #{inspect(target_node)} local ETS for #{inspect(key)} timed out.\n" <>
            "  expected: #{inspect(expected)}\n" <>
            "  got:      #{inspect(got)}"
        )

      true ->
        Process.sleep(50)
        do_assert_node_has(target_node, key, partition_value, expected, deadline)
    end
  end

  @doc "Return the primary node for `partition_value`'s partition."
  @spec primary_for(term) :: node
  def primary_for(partition_value) do
    idx = SuperCache.Partition.get_partition_order(partition_value)
    {primary, _} = Manager.get_replicas(idx)
    primary
  end

  @doc """
  Delete all records on every live node by iterating partitions directly,
  bypassing the router.  Wraps every `:erpc` in `try/catch` so a node that
  has been stopped and may still linger in `Manager.live_nodes/0` does not
  cascade-fail the setup callback.
  """
  @spec safe_delete_all() :: :ok
  def safe_delete_all do
    num = SuperCache.Config.get_config(:num_partition)

    Enum.each(Manager.live_nodes(), fn n ->
      Enum.each(0..(num - 1), fn idx ->
        try do
          partition =
            :erpc.call(n, SuperCache.Partition, :get_partition_by_idx, [idx], 5_000)

          :erpc.call(n, SuperCache.Storage, :delete_all, [partition], 5_000)
        catch
          kind, reason ->
            Logger.debug(
              "safe_delete_all: skipping #{inspect(n)} partition #{idx} — " <>
                "#{inspect({kind, reason})}"
            )
        end
      end)
    end)
  end

  # ── Wait helpers ────────────────────────────────────────────────────────────

  @doc """
  Wait until every node in `Manager.live_nodes/0` reports the same
  `live_nodes` count via `:erpc`.

  If `expected_nodes` is `nil` the current local count is used as the
  target.  Returns `true` when stable, `false` on timeout (5 s).
  """
  @spec wait_cluster_stable(non_neg_integer | nil) :: boolean
  def wait_cluster_stable(expected_nodes \\ nil) do
    n = expected_nodes || length(Manager.live_nodes())

    wait_until(
      fn ->
        Enum.all?(Manager.live_nodes(), fn node ->
          try do
            :erpc.call(node, Manager, :live_nodes, [], 3_000) |> length() == n
          catch
            _, _ -> false
          end
        end)
      end,
      5_000
    )
  end

  @doc """
  Poll `fun` every `interval` ms until it returns a truthy value or
  `timeout` ms elapses.  Returns `true` on success, `false` on timeout.
  """
  @spec wait_until((() -> boolean), non_neg_integer, non_neg_integer) :: boolean
  def wait_until(fun, timeout \\ 3_000, interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline, interval)
  end

  defp do_wait(fun, deadline, interval) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(interval)
        do_wait(fun, deadline, interval)
      end
    end
  end

  # ── ExUnit case template ────────────────────────────────────────────────────

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :cluster
      @moduletag timeout: 120_000

      import SuperCache.ClusterCase

      alias SuperCache.Cluster.Manager
      alias SuperCache.Cluster.NodeMonitor
      alias SuperCache.Distributed, as: Cache

      require Logger

      @cache_opts SuperCache.ClusterCase.cache_opts()

      # Refresh peer/node refs from the agent before every test.
      #
      # setup_all captures peer1/node1/peer2/node2 at startup and returns them
      # as the initial ExUnit context.  Tests that restart a peer update the
      # agent with new handles, but the ExUnit context is FROZEN at setup_all
      # time — pattern-matching %{peer2: peer2} in a subsequent test yields the
      # OLD, dead handle.  Reading from the agent here always gives fresh refs.
      setup %{agent: agent} do
        %{node1: node1, node2: node2, peer1: peer1, peer2: peer2} =
          Agent.get(agent, & &1)

        safe_delete_all()
        Process.sleep(100)

        {:ok, node1: node1, node2: node2, peer1: peer1, peer2: peer2}
      end
    end
  end
end
