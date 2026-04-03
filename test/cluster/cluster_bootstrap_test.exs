defmodule SuperCache.Cluster.BootstrapVerifyTest do
  @moduledoc """
  Tests for `SuperCache.Cluster.Bootstrap` — covering:

  * `export_config/0`   — shape, content, excluded keys
  * `running?/0`        — lifecycle transitions
  * `validate!/1`       — option validation (exercised via start!/1):
      - required keys (:key_pos, :partition_pos)
      - :replication_mode values
      - node-source options (:nodes, :nodes_mfa, :refresh_ms)
  * `verify_cluster_config!/1` — join-time config verification:
      - happy path (identical opts, single and multiple peers)
      - rejection on any individual mismatched structural key
      - rejection lists ALL mismatched keys in a single error
      - error message contains peer node name, local value, and peer value
      - unreachable peer is skipped rather than blocking bootstrap
      - :started is excluded from verification (liveness flag, not structure)
  * Node-source option forwarding:
      - :nodes, :nodes_mfa, :refresh_ms are accepted and forwarded to NodeMonitor
      - validation errors are raised before any subsystem starts
      - NodeMonitor managed set is restricted to :nodes after Bootstrap.start!/1

  ## Isolation strategy

  Every test that exercises cluster behaviour starts its own short-lived
  `:peer` nodes rather than sharing the three-node cluster used by
  `SuperCache.ClusterTest`.  This keeps tests independent and avoids
  state leakage between the config-mismatch tests (which intentionally
  leave peers in a non-standard configuration) and the main cache tests.

  ### Peer roles

  | Term          | Meaning                                              |
  |---------------|------------------------------------------------------|
  | *seeded peer* | Peer where `Bootstrap.start!/1` has already run.    |
  | *blank peer*  | Peer with the SuperCache OTP app started but         |
  |               | `Bootstrap.start!/1` not yet called, so              |
  |               | `running?()` is false and Manager's live_nodes is    |
  |               | seeded only from `:nodeup` events.                   |
  | *joiner*      | Blank peer that will call `Bootstrap.start!/1` under |
  |               | test control.                                        |
  | *plain peer*  | Erlang node with no SuperCache OTP app — used to     |
  |               | verify NodeMonitor filtering.                        |

  ### Why the blank-peer / seeded-peer split?

  `verify_cluster_config!/1` runs inside `Bootstrap.start!/1`.  To observe
  the verification behaviour in isolation we need at least one peer that is
  already `running?/0 == true` (so `Manager.node_running?` considers it live)
  AND a fresh node that will call `Bootstrap.start!/1` under test control.
  Starting both in the same `start_peer` helper would race: the joiner might
  call `start!/1` before the seeded peer is registered in Manager.
  The `start_seeded_peer / start_blank_peer` helpers enforce the required
  ordering explicitly.
  """

  use ExUnit.Case, async: false

  require Logger

  @moduletag :cluster
  @moduletag timeout: 120_000

  alias SuperCache.Cluster.{Bootstrap, Manager}
  alias SuperCache.Config

  # ── Reference opts ────────────────────────────────────────────────────────────

  # Canonical opts used as the "correct" cluster configuration throughout
  # this test suite.  Any deviation from these values in a joiner's
  # Bootstrap.start!/1 call should be rejected once a seeded peer is live.
  @base_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    num_partition:      8,
    table_type:         :set,
    replication_mode:   :async
  ]

  # The six structural keys Bootstrap.verify_cluster_config!/1 checks.
  # Keep in sync with Bootstrap.@config_keys.
  @config_keys [
    :key_pos,
    :partition_pos,
    :num_partition,
    :table_type,
    :replication_factor,
    :replication_mode
  ]


  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    # Ensure the primary node starts with the correct config so that
    # isolated peer tests do not fail due to stale config from previous runs.
    if SuperCache.started?() do
      SuperCache.Cluster.Bootstrap.stop()
      Process.sleep(100)
    end

    SuperCache.Cluster.Bootstrap.start!(@base_opts)

    on_exit(fn ->
      try do
        SuperCache.Cluster.Bootstrap.stop()
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  # ── Peer lifecycle helpers ────────────────────────────────────────────────────

  # Start a `:peer` node, load the SuperCache code path, start the OTP app,
  # and immediately call `Bootstrap.start!/1` with `opts`.
  defp start_seeded_peer(name, opts \\ @base_opts) do
    {peer, node} = start_blank_peer(name)
    :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    {peer, node}
  end

  # Start a `:peer` node with the OTP app running but `Bootstrap.start!/1`
  # NOT called.
  defp start_blank_peer(name) do
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

    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])

    {peer, node}
  end

  # Start a plain Erlang node with no SuperCache OTP app.
  # Used to verify NodeMonitor filtering: these nodes should never be added
  # to Manager.live_nodes when NodeMonitor is in static :nodes mode.
  defp start_plain_peer(name) do
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

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp connect_and_wait(joiner_node, target_node, timeout \\ 8_000) do
    :erpc.call(joiner_node, Node, :connect, [target_node], 5_000)
    Node.connect(joiner_node)

    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      try do
        target_node in :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
      catch
        _, _ -> false
      end
    end)
    |> Enum.find(fn live? ->
      if live? do
        true
      else
        if System.monotonic_time(:millisecond) >= deadline, do: throw(:timeout)
        Process.sleep(100)
        false
      end
    end)
  catch
    :timeout -> false
  end

  defp safe_remote_stop(node) do
    try do
      :erpc.call(node, Bootstrap, :stop, [], 5_000)
    catch
      _, _ -> :ok
    end
  end

  # Execute `fun` and return the ArgumentError message string.
  # Handles both a locally raised ArgumentError and an ErlangError wrapping
  # a remote ArgumentError delivered via :erpc.call.
  defp erpc_argument_error(fun) do
    try do
      fun.()
      flunk("Expected ArgumentError but call returned without raising")
    rescue
      e in ArgumentError ->
        e.message

      e in ErlangError ->
        case e.original do
          {:exception, %ArgumentError{message: msg}, _} ->
            msg

          other ->
            flunk("Expected remote ArgumentError, got ErlangError: #{inspect(other)}")
        end
    end
  end

  defp wait_until(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn -> fun.() end)
    |> Enum.find(fn
      true -> true
      _ ->
        if System.monotonic_time(:millisecond) >= deadline, do: throw(:timeout)
        Process.sleep(100)
        false
    end)
  catch
    :timeout -> false
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # export_config/0
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.export_config/0" do
    setup do
      if node() == :nonode@nohost,
        do: raise("Cluster tests require a distributed node. Run with: mix test.cluster")

      name = :"export_cfg_#{System.unique_integer([:positive])}"
      {peer, node} = start_seeded_peer(name)
      on_exit(fn -> stop_peer(peer) end)
      {:ok, node: node}
    end

    test "returns a map containing exactly the six structural keys", %{node: node} do
      cfg = :erpc.call(node, Bootstrap, :export_config, [], 5_000)

      assert is_map(cfg),
             "export_config/0 must return a map, got: #{inspect(cfg)}"

      assert MapSet.new(Map.keys(cfg)) == MapSet.new(@config_keys),
             "export_config/0 keys mismatch.\n" <>
               "  expected: #{inspect(Enum.sort(@config_keys))}\n" <>
               "  got:      #{inspect(cfg |> Map.keys() |> Enum.sort())}"
    end

    test "values match the opts passed to start!/1", %{node: node} do
      cfg = :erpc.call(node, Bootstrap, :export_config, [], 5_000)

      for key <- @config_keys do
        expected = Keyword.get(@base_opts, key)
        got      = Map.get(cfg, key)

        assert got == expected,
               "export_config key #{inspect(key)}: expected #{inspect(expected)}, got #{inspect(got)}"
      end
    end

    test ":started is NOT included (liveness flag, not structural config)", %{node: node} do
      cfg = :erpc.call(node, Bootstrap, :export_config, [], 5_000)
      refute Map.has_key?(cfg, :started),
             ":started must not appear in export_config — it is a liveness flag"
    end

    test ":cluster is NOT included (always :distributed in this module)", %{node: node} do
      cfg = :erpc.call(node, Bootstrap, :export_config, [], 5_000)
      refute Map.has_key?(cfg, :cluster),
             ":cluster must not appear in export_config"
    end

    test ":table_prefix is NOT included (crash-detectable before verify)", %{node: node} do
      cfg = :erpc.call(node, Bootstrap, :export_config, [], 5_000)
      refute Map.has_key?(cfg, :table_prefix),
             ":table_prefix must not appear in export_config"
    end

    test "can be called via :erpc without passing anonymous functions", %{node: node} do
      assert is_map(:erpc.call(node, Bootstrap, :export_config, [], 5_000))
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # running?/0
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.running?/0" do
    test "returns false before Bootstrap.start!/1 has been called" do
      {peer, node} = start_blank_peer(:running_blank)
      on_exit(fn -> stop_peer(peer) end)

      result = :erpc.call(node, Bootstrap, :running?, [], 5_000)

      refute result,
             "running?() must return false before start!/1 is called, got: #{inspect(result)}"
    end

    test "returns true after a successful Bootstrap.start!/1" do
      {peer, node} = start_seeded_peer(:running_seeded)
      on_exit(fn -> stop_peer(peer) end)

      assert :erpc.call(node, Bootstrap, :running?, [], 5_000),
             "running?() must return true after start!/1 completes"
    end

    test "returns false after Bootstrap.stop/0" do
      {peer, node} = start_seeded_peer(:running_stop)
      on_exit(fn -> stop_peer(peer) end)

      :erpc.call(node, Bootstrap, :stop, [], 5_000)

      refute :erpc.call(node, Bootstrap, :running?, [], 5_000),
             "running?() must return false after stop/0"
    end

    test "transitions: false → true → false across start/stop lifecycle" do
      {peer, node} = start_blank_peer(:running_lifecycle)
      on_exit(fn -> stop_peer(peer) end)

      refute :erpc.call(node, Bootstrap, :running?, [], 5_000)

      :erpc.call(node, Bootstrap, :start!, [@base_opts], 15_000)
      assert :erpc.call(node, Bootstrap, :running?, [], 5_000)

      :erpc.call(node, Bootstrap, :stop, [], 5_000)
      refute :erpc.call(node, Bootstrap, :running?, [], 5_000)
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # validate!/1 (exercised through start!/1)
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.start!/1 option validation" do
    test "raises ArgumentError when :key_pos is missing" do
      {peer, node} = start_blank_peer(:val_no_key_pos)
      on_exit(fn -> stop_peer(peer) end)

      bad_opts = Keyword.delete(@base_opts, :key_pos)

      msg = erpc_argument_error(fn -> :erpc.call(node, Bootstrap, :start!, [bad_opts], 15_000) end)
      assert msg =~ ":key_pos",
             "Error must mention :key_pos, got: #{msg}"
    end

    test "raises ArgumentError when :partition_pos is missing" do
      {peer, node} = start_blank_peer(:val_no_part_pos)
      on_exit(fn -> stop_peer(peer) end)

      bad_opts = Keyword.delete(@base_opts, :partition_pos)

      msg = erpc_argument_error(fn -> :erpc.call(node, Bootstrap, :start!, [bad_opts], 15_000) end)
      assert msg =~ ":partition_pos",
             "Error must mention :partition_pos, got: #{msg}"
    end

    test "raises ArgumentError for an invalid :replication_mode" do
      {peer, node} = start_blank_peer(:val_bad_rep_mode)
      on_exit(fn -> stop_peer(peer) end)

      bad_opts = Keyword.put(@base_opts, :replication_mode, :lazy)

      msg = erpc_argument_error(fn -> :erpc.call(node, Bootstrap, :start!, [bad_opts], 15_000) end)
      assert msg =~ ":replication_mode",
             "Error must mention :replication_mode, got: #{msg}"
    end

    test "accepts :async replication_mode" do
      {peer, node} = start_blank_peer(:val_async)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :replication_mode, :async)
      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "accepts :sync replication_mode" do
      {peer, node} = start_blank_peer(:val_sync)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :replication_mode, :sync)
      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "accepts :strong replication_mode" do
      {peer, node} = start_blank_peer(:val_strong)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :replication_mode, :strong)
      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test ":started is set to true after a successful start!/1" do
      {peer, node} = start_blank_peer(:val_started_flag)
      on_exit(fn -> stop_peer(peer) end)

      :erpc.call(node, Bootstrap, :start!, [@base_opts], 15_000)

      assert :erpc.call(node, Config, :get_config, [:started], 5_000) == true,
             ":started must be true after start!/1 returns :ok"
    end

    test ":started remains false when start!/1 raises (no partial commit)" do
      {peer, node} = start_blank_peer(:val_started_no_commit)
      on_exit(fn -> stop_peer(peer) end)

      bad_opts = Keyword.delete(@base_opts, :key_pos)

      try do
        :erpc.call(node, Bootstrap, :start!, [bad_opts], 15_000)
      catch
        :error, %ArgumentError{} -> :ok
        :error, {:exception, %ArgumentError{}, _} -> :ok
      end

      result = :erpc.call(node, Config, :get_config, [:started, false], 5_000)

      refute result,
             ":started must not be set to true when start!/1 raises"
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # Node-source option validation and forwarding
  #
  # Bootstrap.start!/1 now accepts :nodes, :nodes_mfa, and :refresh_ms and
  # forwards them to NodeMonitor.reconfigure/1 early in the start sequence.
  #
  # Two properties are tested here:
  #
  #   1. **Validation** — invalid combinations or types raise ArgumentError
  #      before any subsystem starts, in the same call to start!/1.
  #
  #   2. **Forwarding / integration** — when valid node-source opts are
  #      given, NodeMonitor's managed set is updated accordingly.  This is
  #      verified by connecting an unmanaged plain Erlang node and asserting
  #      it never appears in Manager.live_nodes.
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.start!/1 node-source option validation" do
    # All tests in this block run on isolated blank peers (no live cluster
    # peers), so verify_cluster_config!/1 is never reached and any
    # ArgumentError comes purely from validate_node_source!/1.

    test "accepts :nodes as a list of node atoms" do
      {peer, node} = start_blank_peer(:ns_nodes_ok)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :nodes, [:"a@127.0.0.1"])
      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "accepts :nodes_mfa as a valid {module, function, args} tuple" do
      {peer, node} = start_blank_peer(:ns_mfa_ok)
      on_exit(fn -> stop_peer(peer) end)

      # Node.list/0 returns a plain list of atoms — a safe no-side-effect MFA.
      opts = Keyword.put(@base_opts, :nodes_mfa, {Node, :list, []})
      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "accepts :refresh_ms as a positive integer alongside :nodes" do
      {peer, node} = start_blank_peer(:ns_refresh_ok)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:refresh_ms, 10_000)

      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "accepts :refresh_ms as a positive integer alongside :nodes_mfa" do
      {peer, node} = start_blank_peer(:ns_refresh_mfa_ok)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes_mfa, {Node, :list, []})
        |> Keyword.put(:refresh_ms, 5_000)

      assert :ok == :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
    end

    test "raises when both :nodes and :nodes_mfa are specified" do
      {peer, node} = start_blank_peer(:ns_both_bad)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:nodes_mfa, {Node, :list, []})

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      # Both keys must be named so the operator knows which pair conflicts.
      assert msg =~ ":nodes",     "Error must mention :nodes, got: #{msg}"
      assert msg =~ ":nodes_mfa", "Error must mention :nodes_mfa, got: #{msg}"
    end

    test "raises when :nodes contains a non-atom element" do
      {peer, node} = start_blank_peer(:ns_nodes_bad_elem)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :nodes, ["not_an_atom"])

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":nodes",
             "Error must mention :nodes, got: #{msg}"
    end

    test "raises when :nodes is not a list" do
      {peer, node} = start_blank_peer(:ns_nodes_bad_type)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :nodes, :"a@127.0.0.1")

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":nodes",
             "Error must mention :nodes, got: #{msg}"
    end

    test "raises when :nodes_mfa is not a {module, function, args} tuple" do
      {peer, node} = start_blank_peer(:ns_mfa_bad_value)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :nodes_mfa, :bad_value)

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":nodes_mfa",
             "Error must mention :nodes_mfa, got: #{msg}"
    end

    test "raises when :nodes_mfa args element is not a list" do
      {peer, node} = start_blank_peer(:ns_mfa_bad_args)
      on_exit(fn -> stop_peer(peer) end)

      opts = Keyword.put(@base_opts, :nodes_mfa, {Node, :list, :not_a_list})

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":nodes_mfa",
             "Error must mention :nodes_mfa, got: #{msg}"
    end

    test "raises when :refresh_ms is zero" do
      {peer, node} = start_blank_peer(:ns_refresh_zero)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:refresh_ms, 0)

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":refresh_ms",
             "Error must mention :refresh_ms, got: #{msg}"
    end

    test "raises when :refresh_ms is a negative integer" do
      {peer, node} = start_blank_peer(:ns_refresh_neg)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:refresh_ms, -1_000)

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":refresh_ms",
             "Error must mention :refresh_ms, got: #{msg}"
    end

    test "raises when :refresh_ms is not an integer" do
      {peer, node} = start_blank_peer(:ns_refresh_float)
      on_exit(fn -> stop_peer(peer) end)

      opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:refresh_ms, 5.0)

      msg = erpc_argument_error(fn ->
        :erpc.call(node, Bootstrap, :start!, [opts], 15_000)
      end)

      assert msg =~ ":refresh_ms",
             "Error must mention :refresh_ms, got: #{msg}"
    end

    test "node-source validation error is raised before any ETS table is created" do
      # When validate_node_source! fails, no subsystem has started yet.
      # A second start!/1 with corrected opts must succeed without hitting
      # "table already exists" from a prior partial start.
      {peer, node} = start_blank_peer(:ns_no_partial_start)
      on_exit(fn -> stop_peer(peer) end)

      bad_opts =
        @base_opts
        |> Keyword.put(:nodes, [:"a@127.0.0.1"])
        |> Keyword.put(:nodes_mfa, {Node, :list, []})  # mutually exclusive

      try do
        :erpc.call(node, Bootstrap, :start!, [bad_opts], 15_000)
      catch
        :error, %ArgumentError{} -> :ok
        :error, {:exception, %ArgumentError{}, _} -> :ok
      end

      # Retry with valid opts — must succeed, not crash with :already_exists.
      assert :ok == :erpc.call(node, Bootstrap, :start!, [@base_opts], 15_000),
             "Second start!/1 with valid opts must succeed after a node-source validation failure"
    end
  end

  describe "Bootstrap.start!/1 node-source option forwarding" do
    # Integration tests: verify that node-source opts passed to Bootstrap.start!/1
    # actually reach NodeMonitor, restricting which Erlang nodes are forwarded
    # to Manager.  Uses a plain Erlang peer (no SuperCache) as the "outsider"
    # that a correctly configured NodeMonitor should silently ignore.

    test ":nodes forwarded — unmanaged plain Erlang node is filtered by NodeMonitor" do
      # Bootstrap a peer with :nodes = [] (empty after connect_all strips self).
      # An unmanaged plain node that Erlang-connects to it must NOT appear in
      # Manager.live_nodes because NodeMonitor's watched?/2 gate filters it.
      {blank_peer, blank_node} = start_blank_peer(:ns_fwd_blank)
      {plain_peer, plain_node} = start_plain_peer(:ns_fwd_plain)

      on_exit(fn ->
        stop_peer(blank_peer)
        stop_peer(plain_peer)
      end)

      # Start with an empty managed set — only node() itself, which connect_all
      # strips, giving an effectively empty MapSet.
      opts = Keyword.put(@base_opts, :nodes, [])
      :erpc.call(blank_node, Bootstrap, :start!, [opts], 15_000)

      # Connect the plain node at the Erlang level; this fires a :nodeup event
      # on blank_node's NodeMonitor.  Because plain_node is not in the managed
      # set, watched?/2 returns false and Manager.node_up is never called.
      :erpc.call(blank_node, Node, :connect, [plain_node], 5_000)
      Process.sleep(300)

      live = :erpc.call(blank_node, Manager, :live_nodes, [], 5_000)

      refute plain_node in live,
             "Unmanaged plain_node #{inspect(plain_node)} appeared in Manager.live_nodes " <>
               "on #{inspect(blank_node)} — NodeMonitor should have filtered it. " <>
               "live_nodes: #{inspect(live)}"
    end

    test ":nodes forwarded — managed node IS added to Manager after connect" do
      # Symmetric complement of the previous test: a node that IS in the
      # :nodes list must be added to Manager when it connects.
      {seed_peer, seed_node}   = start_seeded_peer(:ns_fwd_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:ns_fwd_joiner)

      on_exit(fn ->
        stop_peer(seed_peer)
        stop_peer(joiner_peer)
      end)

      # Bootstrap joiner with :nodes = [seed_node] — seed_node is managed.
      opts = Keyword.put(@base_opts, :nodes, [seed_node])
      :erpc.call(joiner_node, Bootstrap, :start!, [opts], 15_000)

      # Connect at Erlang level; :nodeup fires; NodeMonitor forwards it.
      :erpc.call(joiner_node, Node, :connect, [seed_node], 5_000)

      # Manager may need a moment to complete the health-check retry loop.
      assert wait_until(
               fn ->
                 try do
                   seed_node in :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 catch
                   _, _ -> false
                 end
               end,
               8_000
             ),
             "Managed seed_node #{inspect(seed_node)} did not appear in Manager.live_nodes " <>
               "on #{inspect(joiner_node)} within 8 s"
    end

    test ":nodes_mfa forwarded — MFA result drives initial managed set" do
      # Use Node.list/0 as the MFA.  On a freshly started blank peer with no
      # other Erlang connections, Node.list() returns [].  We then connect a
      # seeded peer and verify that the subsequent refresh tick picks it up.
      {seed_peer, seed_node}   = start_seeded_peer(:ns_mfa_fwd_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:ns_mfa_fwd_joiner)

      on_exit(fn ->
        stop_peer(seed_peer)
        stop_peer(joiner_peer)
      end)

      # Bootstrap with MFA = Node.list/0 and a short refresh interval so the
      # test does not have to wait long for the tick to fire.
      opts =
        @base_opts
        |> Keyword.put(:nodes_mfa, {Node, :list, []})
        |> Keyword.put(:refresh_ms, 300)

      :erpc.call(joiner_node, Bootstrap, :start!, [opts], 15_000)

      # Now connect the seed peer; Node.list() on joiner_node will return
      # [seed_node] on the next refresh tick.
      :erpc.call(joiner_node, Node, :connect, [seed_node], 5_000)

      assert wait_until(
               fn ->
                 try do
                   seed_node in :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 catch
                   _, _ -> false
                 end
               end,
               5_000
             ),
             "seed_node #{inspect(seed_node)} did not appear in Manager.live_nodes " <>
               "on #{inspect(joiner_node)} after MFA refresh"
    end

    test "omitting all node-source keys falls back to :all mode (legacy behaviour)" do
      # When neither :nodes nor :nodes_mfa is given, NodeMonitor should be in
      # :all mode — every Erlang :nodeup event is forwarded to Manager.
      # We verify this by connecting a seeded peer and checking it is added
      # without any explicit reconfigure call.
      {seed_peer, seed_node}   = start_seeded_peer(:ns_all_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:ns_all_joiner)

      on_exit(fn ->
        stop_peer(seed_peer)
        stop_peer(joiner_peer)
      end)

      # Bootstrap with no node-source key — NodeMonitor falls back to :all.
      :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000)

      :erpc.call(joiner_node, Node, :connect, [seed_node], 5_000)

      assert wait_until(
               fn ->
                 try do
                   seed_node in :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 catch
                   _, _ -> false
                 end
               end,
               8_000
             ),
             "In :all mode, seed_node #{inspect(seed_node)} must be added automatically"
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # verify_cluster_config!/1 — happy paths
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.start!/1 config verification — happy path" do
    test "first node in cluster skips verification and starts successfully" do
      {peer, node} = start_blank_peer(:first_node)
      on_exit(fn -> stop_peer(peer) end)

      assert :ok == :erpc.call(node, Bootstrap, :start!, [@base_opts], 15_000),
             "First node should start without verification"

      assert :erpc.call(node, Bootstrap, :running?, [], 5_000),
             "running?() must be true after successful start!/1 on first node"
    end

    test "joining node with identical opts to a single seeded peer succeeds" do
      {seed_peer, seed_node} = start_seeded_peer(:hp_seed_1)
      {joiner_peer, joiner_node} = start_blank_peer(:hp_joiner_1)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      connected = connect_and_wait(joiner_node, seed_node)

      assert connected,
             "Joiner's Manager never saw #{inspect(seed_node)} as live — test precondition failed"

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000)
      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000)
    end

    test "joining node with identical opts to multiple seeded peers succeeds" do
      {seed_peer1, seed_node1} = start_seeded_peer(:hp_seed_multi_1)
      {seed_peer2, seed_node2} = start_seeded_peer(:hp_seed_multi_2)
      {joiner_peer, joiner_node} = start_blank_peer(:hp_joiner_multi)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed_peer1)
        stop_peer(seed_peer2)
      end)

      :erpc.call(joiner_node, Node, :connect, [seed_node1], 5_000)
      :erpc.call(joiner_node, Node, :connect, [seed_node2], 5_000)

      assert wait_until(fn ->
               try do
                 live = :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 seed_node1 in live and seed_node2 in live
               catch
                 _, _ -> false
               end
             end, 8_000),
             "Joiner Manager never reached both seeded peers"

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000)
      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000)
    end

    test ":started flag on the seeded peer is ignored during verification" do
      {seed_peer, seed_node} = start_seeded_peer(:hp_started_ignore_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:hp_started_ignore_joiner)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node),
             "Test precondition failed — joiner never saw seed as live"

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000)
    end

    test "node-source opts do not affect config verification result" do
      # A joiner that passes :nodes alongside structural opts must still pass
      # verification — node-source keys are not in @config_keys and are never
      # compared against peers.
      {seed_peer, seed_node} = start_seeded_peer(:hp_ns_verify_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:hp_ns_verify_joiner)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node),
             "Test precondition failed"

      opts = Keyword.put(@base_opts, :nodes, [seed_node])
      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [opts], 15_000),
             "Presence of :nodes in opts must not affect config verification"

      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000)
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # verify_cluster_config!/1 — mismatch rejection
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.start!/1 config verification — mismatch rejection" do
    defp assert_mismatch_raises(seed_name, joiner_name, joiner_opts) do
      {seed_peer, seed_node} = start_seeded_peer(seed_name)
      {joiner_peer, joiner_node} = start_blank_peer(joiner_name)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node),
             "Test precondition failed: joiner #{inspect(joiner_node)} never " <>
               "saw #{inspect(seed_node)} as live"

      erpc_argument_error(fn ->
        :erpc.call(joiner_node, Bootstrap, :start!, [joiner_opts], 15_000)
      end)
    end

    test "raises ArgumentError when :num_partition differs" do
      wrong_opts = Keyword.put(@base_opts, :num_partition, 4)
      msg = assert_mismatch_raises(:mm_np_seed, :mm_np_joiner, wrong_opts)

      assert msg =~ "num_partition",
             "Error message must name :num_partition, got: #{msg}"

      assert msg =~ "config mismatch",
             "Error message must say 'config mismatch', got: #{msg}"
    end

    test "raises ArgumentError when :replication_factor differs" do
      wrong_opts = Keyword.put(@base_opts, :replication_factor, 3)
      msg = assert_mismatch_raises(:mm_rf_seed, :mm_rf_joiner, wrong_opts)

      assert msg =~ "replication_factor",
             "Error message must name :replication_factor, got: #{msg}"
    end

    test "raises ArgumentError when :replication_mode differs" do
      wrong_opts = Keyword.put(@base_opts, :replication_mode, :sync)
      msg = assert_mismatch_raises(:mm_rm_seed, :mm_rm_joiner, wrong_opts)

      assert msg =~ "replication_mode",
             "Error message must name :replication_mode, got: #{msg}"
    end

    test "raises ArgumentError when :key_pos differs" do
      wrong_opts = Keyword.put(@base_opts, :key_pos, 1)
      msg = assert_mismatch_raises(:mm_kp_seed, :mm_kp_joiner, wrong_opts)

      assert msg =~ "key_pos",
             "Error message must name :key_pos, got: #{msg}"
    end

    test "raises ArgumentError when :partition_pos differs" do
      wrong_opts = Keyword.put(@base_opts, :partition_pos, 1)
      msg = assert_mismatch_raises(:mm_pp_seed, :mm_pp_joiner, wrong_opts)

      assert msg =~ "partition_pos",
             "Error message must name :partition_pos, got: #{msg}"
    end

    test "raises ArgumentError when :table_type differs" do
      wrong_opts = Keyword.put(@base_opts, :table_type, :bag)
      msg = assert_mismatch_raises(:mm_tt_seed, :mm_tt_joiner, wrong_opts)

      assert msg =~ "table_type",
             "Error message must name :table_type, got: #{msg}"
    end

    test "error message includes the disagreeing peer's node name" do
      {seed_peer, seed_node} = start_seeded_peer(:mm_peer_name_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:mm_peer_name_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      wrong_opts = Keyword.put(@base_opts, :num_partition, 4)

      msg =
        erpc_argument_error(fn ->
          :erpc.call(joiner_node, Bootstrap, :start!, [wrong_opts], 15_000)
        end)

      assert msg =~ inspect(seed_node),
             "Error message must name the disagreeing peer #{inspect(seed_node)}, got:\n#{msg}"
    end

    test "error message shows both local value and peer value for each mismatched key" do
      {seed_peer, seed_node} = start_seeded_peer(:mm_vals_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:mm_vals_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      wrong_opts = Keyword.put(@base_opts, :num_partition, 4)

      msg =
        erpc_argument_error(fn ->
          :erpc.call(joiner_node, Bootstrap, :start!, [wrong_opts], 15_000)
        end)

      assert msg =~ "4",
             "Error message must show local value 4, got:\n#{msg}"

      assert msg =~ "8",
             "Error message must show peer value 8, got:\n#{msg}"
    end

    test "all mismatched keys appear together in a single error message" do
      {seed_peer, seed_node} = start_seeded_peer(:mm_multi_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:mm_multi_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      wrong_opts =
        @base_opts
        |> Keyword.put(:num_partition, 4)
        |> Keyword.put(:replication_factor, 3)
        |> Keyword.put(:replication_mode, :sync)
        |> Keyword.put(:table_type, :bag)

      msg =
        erpc_argument_error(fn ->
          :erpc.call(joiner_node, Bootstrap, :start!, [wrong_opts], 15_000)
        end)

      for key <- [:num_partition, :replication_factor, :replication_mode, :table_type] do
        assert msg =~ to_string(key),
               "Error message must include #{inspect(key)}, got:\n#{msg}"
      end
    end

    test ":started mismatch does NOT cause rejection" do
      {seed_peer, seed_node} = start_seeded_peer(:mm_started_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:mm_started_joiner)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000),
             "Difference in :started must not cause rejection"

      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000)
    end

    test "only the disagreeing peer appears in the error — agreeing peers are not listed" do
      {seed_ok_peer, seed_ok_node} = start_seeded_peer(:mm_agree_ok_seed)
      {seed_bad_peer, seed_bad_node} =
        start_seeded_peer(:mm_agree_bad_seed, Keyword.put(@base_opts, :num_partition, 4))
      {joiner_peer, joiner_node} = start_blank_peer(:mm_agree_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_ok_peer)
        stop_peer(seed_bad_peer)
      end)

      :erpc.call(joiner_node, Node, :connect, [seed_ok_node], 5_000)
      :erpc.call(joiner_node, Node, :connect, [seed_bad_node], 5_000)

      assert wait_until(fn ->
               try do
                 live = :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 seed_ok_node in live and seed_bad_node in live
               catch
                 _, _ -> false
               end
             end, 8_000),
             "Joiner never saw both seed peers as live"

      msg =
        erpc_argument_error(fn ->
          :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000)
        end)

      assert msg =~ inspect(seed_bad_node),
             "Error must name the disagreeing peer #{inspect(seed_bad_node)}"

      refute msg =~ inspect(seed_ok_node),
             "Agreeing peer #{inspect(seed_ok_node)} must not appear in error"
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # verify_cluster_config!/1 — resilience
  # ═════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap.start!/1 config verification — resilience" do
    test "unreachable peer is skipped and does not block a valid-config join" do
      {seed1_peer, seed1_node} = start_seeded_peer(:res_seed1)
      {seed2_peer, seed2_node} = start_seeded_peer(:res_seed2)
      {joiner_peer, joiner_node} = start_blank_peer(:res_joiner)

      on_exit(fn ->
        stop_peer(joiner_peer)
        stop_peer(seed1_peer)
        stop_peer(seed2_peer)
      end)

      :erpc.call(joiner_node, Node, :connect, [seed1_node], 5_000)
      :erpc.call(joiner_node, Node, :connect, [seed2_node], 5_000)

      assert wait_until(fn ->
               try do
                 live = :erpc.call(joiner_node, Manager, :live_nodes, [], 3_000)
                 seed1_node in live and seed2_node in live
               catch
                 _, _ -> false
               end
             end, 8_000),
             "Joiner never saw both seeds as live — test precondition failed"

      stop_peer(seed2_peer)
      Process.sleep(200)

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000),
             "Bootstrap should succeed when the only unreachable peer had correct config"

      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000)
    end

    test "node remains not-started (:started == false) when verification fails" do
      {seed_peer, seed_node} = start_seeded_peer(:res_not_started_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:res_not_started_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      wrong_opts = Keyword.put(@base_opts, :num_partition, 4)

      try do
        :erpc.call(joiner_node, Bootstrap, :start!, [wrong_opts], 15_000)
      catch
        :error, %ArgumentError{} -> :ok
        :error, {:exception, %ArgumentError{}, _} -> :ok
      end

      refute :erpc.call(joiner_node, Config, :get_config, [:started, false], 5_000),
             ":started must remain false after a rejected start!/1"

      refute :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000),
             "running?() must return false after a rejected start!/1"
    end

    test "a second start!/1 with correct opts succeeds after an initial rejection" do
      {seed_peer, seed_node} = start_seeded_peer(:res_retry_seed)
      {joiner_peer, joiner_node} = start_blank_peer(:res_retry_joiner)

      on_exit(fn ->
        safe_remote_stop(joiner_node)
        stop_peer(joiner_peer)
        stop_peer(seed_peer)
      end)

      assert connect_and_wait(joiner_node, seed_node)

      wrong_opts = Keyword.put(@base_opts, :num_partition, 4)

      try do
        :erpc.call(joiner_node, Bootstrap, :start!, [wrong_opts], 15_000)
      catch
        :error, %ArgumentError{} -> :ok
        :error, {:exception, %ArgumentError{}, _} -> :ok
      end

      refute :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000),
             "Precondition: node must not be running after rejected start!/1"

      assert :ok == :erpc.call(joiner_node, Bootstrap, :start!, [@base_opts], 15_000),
             "Second start!/1 with correct opts must succeed after prior rejection"

      assert :erpc.call(joiner_node, Bootstrap, :running?, [], 5_000),
             "running?() must be true after successful retry"
    end
  end
end
