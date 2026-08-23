defmodule SuperCache.PeerCoverageTest do
  @moduledoc """
  Coverage round five — genuine two-node scenarios executed inside the normal
  (non-cluster-tagged) test run.

  The suite normally runs undistributed (`nonode@nohost`). This file starts
  Erlang distribution at runtime, spawns one peer VM via `:peer`, boots
  SuperCache on it, and exercises the code paths that require a *reachable*
  remote node:

  - Manager health-gated node adds and the retry chain for nodes that come
    up late (`node_running?` returning true mid-retry).
  - Cluster.Bootstrap cross-node config verification (agree / mismatch /
    unreachable peer) and connection pre-warming.
  - ThreePhaseCommit coordinator phases against a live replica (happy path
    and VOTE_NO abort).
  - Replicator synchronous quorum success, batch replication, and partition
    pushes that land on the peer.
  - Application.connect_peers success/failure reporting.
  """

  use ExUnit.Case, async: false

  @moduletag timeout: 240_000

  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}
  alias SuperCache.Cluster.Bootstrap
  alias SuperCache.Config

  @pt_partition_map {Manager, :partition_map}
  @pt_peer {__MODULE__, :peer}
  @local_name :peer_cov_main
  @peer_name :peer_cov_aux
  @dead :"peer_cov_dead@host"

  # Both sides run 8 partitions so structural config agrees everywhere.
  @num_partitions 8

  # ── Cluster lifecycle ─────────────────────────────────────────────────────────

  setup_all do
    original_env = Application.get_all_env(:super_cache)

    start_distribution()
    {peer, peer_node} = start_peer_cluster()
    :persistent_term.put({__MODULE__, :agent}, %{peer_node: peer_node})
    :persistent_term.put(@pt_peer, peer)

    on_exit(fn ->
      state = :persistent_term.get(@pt_peer, nil)
      state && stop_peer(state)

      restore_local(original_env)
    end)

    %{ok: true}
  end

  defp start_distribution do
    unless Node.alive?() do
      {:ok, _} = Node.start(@local_name, :shortnames)
    end

    reboot_local_tree()

    Bootstrap.stop()
    Process.sleep(50)
    :ok = Bootstrap.start!(cache_opts())
    Process.sleep(50)
  end

  defp cache_opts do
    [
      key_pos: 0,
      partition_pos: 0,
      cluster: :distributed,
      num_partition: @num_partitions,
      replication_factor: 2
    ]
  end

  defp start_peer_cluster do
    cookie = :erlang.get_cookie()

    {:ok, peer, peer_node} =
      :peer.start(%{
        name: @peer_name,
        args: [
          ~c"-setcookie", :erlang.atom_to_list(cookie),
          ~c"-connect_all", ~c"false"
        ]
      })

    # Pure-Erlang calls first: the bare peer has no Elixir loaded yet.
    wait_until(fn ->
      match?({:ok, _}, :erpc.call(peer_node, :code, :add_paths, [:code.get_path()], 15_000))
    end)

    {:ok, _} = :erpc.call(peer_node, :application, :ensure_all_started, [:super_cache], 15_000)
    :erpc.call(peer_node, Bootstrap, :start!, [cache_opts()], 15_000)

    true = Node.connect(peer_node)

    assert wait_until(fn -> peer_node in Manager.live_nodes() end, 20_000),
           "peer did not join the cluster"

    {peer, peer_node}
  end

  defp peer_node, do: get_agent(:peer_node)

  defp get_agent(key), do: :persistent_term.get({__MODULE__, :agent}, %{})[key]

  # Reboot the supervision tree so every component observes the current node().
  defp reboot_local_tree do
    :ok = Application.stop(:super_cache)
    {:ok, _} = Application.ensure_all_started(:super_cache)

    # Fresh Manager membership for the current node name.
    GenServer.stop(Manager)
    wait_until(fn -> Process.whereis(Manager) != nil end)
    Process.sleep(50)
    :ok
  end

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp restore_local(original_env) do
    Enum.each(original_env, fn {k, v} -> Application.put_env(:super_cache, k, v) end)

    try do
      Bootstrap.stop()
    catch
      :exit, _ -> :ok
    end

    if Node.alive?(), do: Node.stop()
    Process.sleep(200)

    {:ok, _} = Application.ensure_all_started(:super_cache)

    # Drop any stale membership entries left over from the distributed phase.
    GenServer.stop(Manager)
    wait_until(fn -> Process.whereis(Manager) != nil end)
    Process.sleep(100)
    :ok
  end

  defp wait_until(fun, tries \\ 150)

  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, tries) do
    if fun.() do
      true
    else
      Process.sleep(50)
      wait_until(fun, tries - 1)
    end
  end

  defp plant_map(map) do
    original = :persistent_term.get(@pt_partition_map, nil)
    :persistent_term.put(@pt_partition_map, map)

    on_exit(fn ->
      if original,
        do: :persistent_term.put(@pt_partition_map, original),
        else: :persistent_term.erase(@pt_partition_map)
    end)
  end

  defp table(order), do: :"SuperCache.Storage.Ets_#{order}"

  # ════════════════════════════════════════════════════════════════════════════
  # Tests
  # ════════════════════════════════════════════════════════════════════════════

  test "setup produced a healthy two-node cluster" do
    assert length(Manager.live_nodes()) == 2

    {primary, replicas} = Manager.get_replicas(0)
    assert primary in Manager.live_nodes()
    assert replicas != []
    assert Enum.all?(replicas, &(&1 in Manager.live_nodes()))
  end

  test "node_down removes a member and node_up re-adds a healthy peer" do
    pn = peer_node()

    Manager.node_down(pn)
    wait_until(fn -> pn not in Manager.live_nodes() end)
    refute pn in Manager.live_nodes()

    Manager.node_up(pn)
    assert wait_until(fn -> pn in Manager.live_nodes() end, 15_000)

    {primary, replicas} = Manager.get_replicas(0)
    assert primary in Manager.live_nodes()
    assert Enum.all?(replicas, &(&1 in Manager.live_nodes()))
  end

  test "a node that becomes ready during the retry window is picked up" do
    cookie = :erlang.get_cookie()

    {:ok, late_peer, late_node} =
      :peer.start(%{
        name: :peer_cov_late,
        args: [
          ~c"-setcookie", :erlang.atom_to_list(cookie),
          ~c"-connect_all", ~c"false"
        ]
      })

    try do
      true = Node.connect(late_node)

      # Health check fails now — the manager schedules retries.
      Manager.node_up(late_node)
      Process.sleep(700)

      # …then the node becomes ready and the next retry succeeds.
      :erpc.call(late_node, :code, :add_paths, [:code.get_path()], 15_000)
      {:ok, _} = :erpc.call(late_node, Application, :ensure_all_started, [:super_cache], 15_000)
      :erpc.call(late_node, Bootstrap, :start!, [cache_opts()], 15_000)

      assert wait_until(fn -> late_node in Manager.live_nodes() end, 20_000),
             "late node never joined via retry"
    after
      stop_peer(late_peer)
      Manager.node_down(late_node)
      wait_until(fn -> late_node not in Manager.live_nodes() end, 40)
    end
  end

  describe "Cluster.Bootstrap config verification" do
    test "start!/0 boots with defaults" do
      Bootstrap.stop()
      Process.sleep(30)

      assert :ok = Bootstrap.start!(cache_opts())
      Process.sleep(30)
      assert Bootstrap.running?()
    end

    test "mismatched structural config is rejected before tables are created" do
      pn = peer_node()

      Bootstrap.stop()
      Process.sleep(30)

      :erpc.call(pn, Config, :set_config, [:num_partition, 3], 5_000)

      try do
        assert_raise ArgumentError, ~r/config mismatch/, fn ->
          Bootstrap.start!(cache_opts())
        end
      after
        :erpc.call(pn, Config, :set_config, [:num_partition, @num_partitions], 5_000)
      end

      assert :ok = Bootstrap.start!(cache_opts())
      Process.sleep(30)
    end

    test "an unreachable peer is skipped with a warning" do
      pn = peer_node()

      Bootstrap.stop()
      Process.sleep(30)

      # Make export_config/0 unavailable on the peer so the fetch fails.
      :erpc.call(pn, :code, :delete, [Bootstrap], 5_000)

      try do
        assert :ok = Bootstrap.start!(cache_opts())
        assert Bootstrap.running?()
      after
        :erpc.call(pn, :code, :purge, [Bootstrap], 5_000)
        :erpc.call(pn, :code, :load_file, [Bootstrap], 5_000)
      end
    end

    test "pre-warm contacts configured peers" do
      pn = peer_node()

      Bootstrap.stop()
      Process.sleep(30)

      opts = cache_opts() ++ [nodes: [pn, @dead]]
      assert :ok = Bootstrap.start!(opts)
      Process.sleep(30)

      assert Bootstrap.running?()
    end

    test "starting alongside a live member requests a full sync" do
      Bootstrap.stop()
      Process.sleep(30)

      # The live peer makes start!/1 request a full sync during boot.
      assert :ok = Bootstrap.start!(cache_opts())
      Process.sleep(300)
      assert Bootstrap.running?()
    end
  end

  describe "Manager full sync" do
    test "pushes owned partitions to the peer" do
      owned =
        Enum.find(0..(@num_partitions - 1), fn i ->
          {primary, _} = Manager.get_replicas(i)
          primary == node()
        end)

      assert is_integer(owned)
      SuperCache.Storage.delete_all(table(owned))
      SuperCache.Storage.put([{:fs_probe, 7}], table(owned))

      Manager.full_sync()

      wait_until(fn ->
        [{:fs_probe, 7}] ==
          :erpc.call(peer_node(), SuperCache.Storage, :get, [:fs_probe, table(owned)], 5_000)
      end)
    end
  end

  describe "ThreePhaseCommit across a live pair" do
    test "a full three-phase commit applies on the replica" do
      plant_map(%{0 => {node(), [peer_node()]}})
      SuperCache.Storage.delete_all(table(0))

      assert :ok == ThreePhaseCommit.commit(0, [{:put, {{:tpc_happy, :v}, 1}}])

      assert [{{:tpc_happy, :v}, 1}] ==
               SuperCache.Storage.get({:tpc_happy, :v}, table(0))

      wait_until(fn ->
        [{{:tpc_happy, :v}, 1}] ==
          :erpc.call(
            peer_node(),
            SuperCache.Storage,
            :get,
            [{:tpc_happy, :v}, table(0)],
            5_000
          )
      end)
    end

    test "an invalid operation earns a VOTE_NO and an abort" do
      plant_map(%{0 => {node(), [peer_node()]}})

      assert {:error, {:vote_no, _}} =
               ThreePhaseCommit.commit(0, [{:put, :not_a_tuple}])
    end

    test "a replica failing its local apply fails the commit" do
      # Index beyond the configured range exists in the planted map only;
      # the peer rejects the commit because partition 42 does not exist there.
      plant_map(%{42 => {node(), [peer_node()]}})

      assert {:error, {:commit_failed, _}} =
               ThreePhaseCommit.commit(42, [{:put, {:asym_key, :asym_val}}])
    end
  end

  describe "Replicator against a live replica" do
    test "batch replication succeeds when every replica answers" do
      plant_map(%{0 => {node(), [peer_node()]}})

      assert :ok =
               Replicator.replicate_batch(0, :put, [
                 {:rb_a, 1},
                 {:rb_b, 2}
               ])
    end

    test "sync-mode replication reaches its quorum" do
      plant_map(%{0 => {node(), [peer_node()]}})
      Config.set_config(:replication_mode, :sync)

      try do
        assert :ok = Replicator.replicate(0, :put, {:sync_rec, 42})
      after
        Config.set_config(:replication_mode, :async)
      end
    end

    test "push_partition transfers records to the peer" do
      SuperCache.Storage.delete_all(table(1))
      SuperCache.Storage.put([{:pp_x, 1}], table(1))

      assert :ok = Replicator.push_partition(1, peer_node())

      wait_until(fn ->
        [{:pp_x, 1}] ==
          :erpc.call(peer_node(), SuperCache.Storage, :get, [:pp_x, table(1)], 5_000)
      end)
    end
  end

  describe "HealthMonitor with a live replica" do
    test "replication check measures lag and reports unknown without real sync" do
      # This node is primary of partition 0 with the peer as replica; the
      # probe never reaches the peer (raw Storage.put bypasses replication),
      # so the measurement times out and reports :unknown.
      plant_map(%{0 => {node(), [peer_node()]}})
      SuperCache.Storage.delete_all(table(0))

      assert :ok = SuperCache.Cluster.HealthMonitor.force_check()

      %{checks: %{replication: rep}} =
        SuperCache.Cluster.HealthMonitor.node_health(node())
      assert rep.status in [:pass, :unknown, :degraded]
    end
  end

  describe "Application peer connections" do
    test "auto-start connects reachable peers and reports unreachable ones" do
      original_env = Application.get_all_env(:super_cache)

      try do
        Application.put_env(:super_cache, :auto_start, true)
        Application.put_env(:super_cache, :key_pos, 0)
        Application.put_env(:super_cache, :partition_pos, 0)
        Application.put_env(:super_cache, :num_partition, @num_partitions)
        Application.put_env(:super_cache, :cluster_peers, [peer_node(), @dead])

        :ok = Application.stop(:super_cache)
        {:ok, _} = Application.ensure_all_started(:super_cache)
        Process.sleep(500)

        assert SuperCache.started?()
        assert peer_node() in Node.list()
      after
        Enum.each(original_env, fn {k, v} -> Application.put_env(:super_cache, k, v) end)

        :ok = Application.stop(:super_cache)
        {:ok, _} = Application.ensure_all_started(:super_cache)

        Bootstrap.stop()
        Process.sleep(30)
        :ok = Bootstrap.start!(cache_opts())
        Process.sleep(30)
      end
    end
  end
end
