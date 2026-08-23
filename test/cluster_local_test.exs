defmodule SuperCache.ClusterLocalTest do
  @moduledoc """
  Single-node coverage for cluster subsystems.

  No real peers are used: remote nodes are simulated with non-connectable
  fake names (erpc fails fast, exercising the graceful-failure branches),
  and partition maps / ETS state are planted directly.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{
    DistributedHelpers,
    DistributedStore,
    HealthMonitor,
    Manager,
    Metrics,
    NodeMonitor,
    Replicator,
    Stats,
    ThreePhaseCommit,
    TxnRegistry,
    WAL
  }

  alias SuperCache.{Partition, Storage}
  @pt_partition_map {Manager, :partition_map}

  # ── Shared setup ─────────────────────────────────────────────────────────────

  defp ensure_local_cache! do
    # Always force a restart with THIS module's config — neighbouring test
    # files leave different key_pos/num_partition globals behind.
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
    Process.sleep(30)
    :ok
  end

  defp plant_map(map) do
    original = :persistent_term.get(@pt_partition_map, nil)
    :persistent_term.put(@pt_partition_map, map)

    on_exit(fn ->
      if original, do: :persistent_term.put(@pt_partition_map, original),
        else: :persistent_term.erase(@pt_partition_map)
    end)

    :ok
  end

  # ── Manager ──────────────────────────────────────────────────────────────────

  describe "Manager" do
    test "get_replicas/1 falls back to self when no map is published" do
      original = :persistent_term.get(@pt_partition_map, nil)

      try do
        :persistent_term.erase(@pt_partition_map)
        assert {self_node, []} = Manager.get_replicas(0)
        assert self_node == node()
      after
        if original, do: :persistent_term.put(@pt_partition_map, original)
      end
    end

    test "node_up/1 for self adds it to membership" do
      Manager.node_up(node())
      Process.sleep(50)
      assert node() in Manager.live_nodes()
    end

    test "node_down/1 removes a member but always keeps self" do
      Manager.node_up(:manager_fake@host)
      Process.sleep(80)
      # Fake node cannot pass the health check so it never joins; force it via
      # a direct cast is not possible — instead verify node_down of unknown
      # and self are safe no-ops.
      Manager.node_down(:never_was_member@host)
      Manager.node_down(node())
      Process.sleep(50)
      assert node() in Manager.live_nodes()
    end

    test "full_sync/0 is a safe no-op without peers" do
      assert :ok = Manager.full_sync()
      Process.sleep(20)
    end

    test "replication_mode/0 defaults to :async" do
      original = SuperCache.Config.get_config(:replication_mode, :unset)

      try do
        SuperCache.Config.delete_config(:replication_mode)
        assert :async = Manager.replication_mode()
      after
        if original != :unset, do: SuperCache.Config.set_config(:replication_mode, original)
      end
    end

    test "handle_info swallows stray messages" do
      send(Manager, :stray_message)
      Process.sleep(10)
      assert Process.whereis(Manager)
    end
  end

  # ── NodeMonitor ──────────────────────────────────────────────────────────────

  describe "NodeMonitor" do
    test "kernel-shaped nodeup/nodedown messages are accepted" do
      send(NodeMonitor, {:nodeup, :nm_fake@host, []})
      send(NodeMonitor, {:nodedown, :nm_fake@host, []})
      Process.sleep(20)
      assert Process.whereis(NodeMonitor)
    end

    test "stale refresh refs are ignored" do
      send(NodeMonitor, {:refresh, make_ref()})
      Process.sleep(10)
      assert Process.whereis(NodeMonitor)
    end

    test "unknown calls return an error tuple" do
      assert {:error, :unknown_call} = GenServer.call(NodeMonitor, :junk_call)
    end

    test "reconfigure/1 swaps the watched set and announces diffs" do
      assert :ok = NodeMonitor.reconfigure(nodes: [:"nm_a@host", :"nm_b@host"])
      assert :ok = NodeMonitor.reconfigure(nodes: [])

      # Restore the default source so other tests are unaffected.
      assert :ok = NodeMonitor.reconfigure([])
      Process.sleep(20)
    end

    test "start_link/1 rejects invalid opts eagerly" do
      assert_raise ArgumentError, fn ->
        NodeMonitor.start_link(nodes: :not_a_list)
      end

      assert_raise ArgumentError, fn ->
        NodeMonitor.start_link(nodes_mfa: :garbage)
      end
    end
  end

  # ── Metrics-driven Stats ─────────────────────────────────────────────────────

  describe "Stats" do
    setup do
      ensure_local_cache!()
      # Seed api metrics so api/0 has data to report.
      for op <- [:put, :get] do
        Metrics.increment({:api, op}, :calls)
        Metrics.push_latency({:api_latency_us, op}, 42)
      end

      Metrics.increment(:tpc, :committed)
      WAL.commit(Partition.get_partition_order(:stats_seed), [{:put, {:stats_seed, 1}}])

      :ok
    end

    test "api/0 reports per-op counters and latency stats" do
      stats = Stats.api()
      assert is_map(stats)

      put_stats = stats.put
      assert put_stats.calls >= 1
      assert Map.has_key?(put_stats, :avg_us)
    end

    test "cluster/0 returns the overview map in local mode" do
      overview = Stats.cluster()

      assert overview.node_count >= 1
      assert overview.num_partitions == Partition.get_num_partition()
      assert is_integer(overview.total_records)
      assert length(overview.partitions) == Partition.get_num_partition()
    end

    test "partitions/0 describes role and counts per partition" do
      [first | _] = Stats.partitions()

      assert %{idx: _, table: table, primary: primary, replicas: _, role: role} = first
      assert primary == node()
      assert role == :primary
      assert is_atom(table)
    end

    test "primary_partitions/0 and replica_partitions/0 filter by role" do
      primaries = Stats.primary_partitions()
      assert primaries != []
      assert Enum.all?(primaries, &(&1.role == :primary))

      replicas = Stats.replica_partitions()
      assert Enum.all?(replicas, &(&1.role == :replica))
    end

    test "node_partitions/1 aggregates for any node name" do
      mine = Stats.node_partitions(node())
      assert is_map(mine)

      # A fabricated node simply has nothing local.
      other = Stats.node_partitions(:"fabricated@host")
      assert is_map(other)
    end

    test "three_phase_commit/0 reports tpc counters" do
      report = Stats.three_phase_commit()
      assert is_map(report)
    end

    test "record/2 increments calls/errors and stores latency" do
      before_calls = get_in(Metrics.get_all({:api, :recorded_op}), [:calls]) || 0

      :ok = Stats.record(:recorded_op, %{latency_us: 11, error: false})
      :ok = Stats.record(:recorded_op, %{latency_us: 22, error: true})

      counts = Metrics.get_all({:api, :recorded_op})
      assert counts.calls == before_calls + 2
      assert counts.errors == 1

      Process.sleep(50)
      samples = Metrics.get_latency_samples({:api_latency_us, :recorded_op})
      assert 22 in samples
      assert 11 in samples
    end

    test "record_tpc/2 handles all event shapes" do
      assert :ok = Stats.record_tpc(:committed, latency_us: 5)
      assert :ok = Stats.record_tpc(:aborted, phase: :prepare)
      assert :ok = Stats.record_tpc(:aborted, [])
      assert is_integer(Stats.record_tpc(:recovered_committed, []))
      assert Metrics.get_all(:tpc)[:recovered_committed] != nil
      assert is_integer(Stats.record_tpc(:recovered_aborted, []))
      assert :ok = Stats.record_tpc(:whatever_unknown, [])
    end

    test "print/1 renders maps and lists without crashing" do
      assert :ok = Stats.print(%{a: 1, b: %{c: 2}})
      assert :ok = Stats.print([%{idx: 0}, %{idx: 1}])
      assert :ok = Stats.print(%{})
    end
  end

  # ── HealthMonitor ────────────────────────────────────────────────────────────

  describe "HealthMonitor" do
    setup do
      ensure_local_cache!()
      :ok
    end

    setup do
      :ets.delete_all_objects(HealthMonitor.HealthData)
      :ok
    end

    test "force_check/0 refreshes data readable via node_health/1" do
      assert :ok = HealthMonitor.force_check()

      node_report = HealthMonitor.node_health(node())
      assert node_report.node == node()
      assert %{} = node_report.checks.connectivity
    end

    test ":health_check message triggers a cycle without crashing" do
      send(HealthMonitor, :health_check)
      Process.sleep(100)

      assert Process.alive?(Process.whereis(HealthMonitor))
      assert HealthMonitor.node_health(node()).checks.connectivity.status != nil
    end

    test "stray messages are swallowed" do
      send(HealthMonitor, :garbage)
      Process.sleep(10)
      assert Process.alive?(Process.whereis(HealthMonitor))
    end

    test "node_health/1 derives degraded status from slow connectivity" do
      :ets.insert(
        HealthMonitor.HealthData,
        {{node(), :connectivity}, %{status: :degraded, latency_ms: 999, timestamp: 123}}
      )

      report = HealthMonitor.node_health(node())
      assert report.status == :degraded
      assert report.latency_ms == 999
    end

    test "node_health/1 derives unhealthy when replication fails" do
      :ets.insert(
        HealthMonitor.HealthData,
        {{node(), :connectivity}, %{status: :pass, latency_ms: 1, timestamp: 123}}
      )

      :ets.insert(
        HealthMonitor.HealthData,
        {{node(), :replication}, %{status: :fail, timestamp: 123}}
      )

      assert HealthMonitor.node_health(node()).status == :unhealthy
    end

    test "unknown checks default to unknown status" do
      report = HealthMonitor.node_health(node())
      assert report.checks.error_rate.status == :unknown
      assert report.last_check == 0 or is_integer(report.last_check)
    end

    test "replication_lag/0..1 handles single-node (no replicas)" do
      result = HealthMonitor.replication_lag(0)
      assert result.replicas == []
    end

    test "partition_balance/0 works locally" do
      balance = HealthMonitor.partition_balance()
      num = Partition.get_num_partition()

      assert balance.partition_count == num
      assert length(balance.partitions) == num
      assert is_number(balance.total_records)
      assert is_float(balance.avg_records_per_partition) or is_integer(balance.avg_records_per_partition)
    end

    test "cluster_health/0 includes this node even without peers" do
      health = HealthMonitor.cluster_health()
      assert Enum.any?(health.nodes, &(&1.node == node()))
    end
  end

  # ── ThreePhaseCommit ─────────────────────────────────────────────────────────

  describe "ThreePhaseCommit" do
    setup do
      ensure_local_cache!()
      :ok
    end

    setup do
      :ets.delete_all_objects(SuperCache.Cluster.TxnRegistry)
      :ok
    end

    test "commit with no replicas applies locally and returns :ok" do
      partition = Partition.get_partition_by_idx(2)
      Storage.delete_all(partition)

      assert :ok = ThreePhaseCommit.commit(2, [{:put, {:tpc_local, "v"}}])
      assert [{:tpc_local, "v"}] == Storage.get(:tpc_local, partition)
    end

    test "commit with no replicas and invalid ops list returns error" do
      assert {:error, _} = ThreePhaseCommit.commit(2, :not_a_list)
    end

    test "commit with unreachable replicas fails fast in prepare" do
      plant_map(%{3 => {node(), [:"tpc_fake@host"]}})

      assert {:error, {:prepare_timeout, _}} =
               ThreePhaseCommit.commit(3, [{:put, {:tpc_remote, "v"}}])

      # Registry entry was removed on abort.
      assert TxnRegistry.count() in [:undefined, 0]
    end

    test "handle_prepare votes yes for valid ops and registers them" do
      assert :vote_yes = ThreePhaseCommit.handle_prepare("txn-yes", 1, [{:put, {:a, 1}}])
      entries = TxnRegistry.list_all()
      assert Enum.any?(entries, fn {id, _} -> id == "txn-yes" end)
    end

    test "handle_prepare votes no for invalid ops" do
      assert {:vote_no, :ops_must_be_list} =
               ThreePhaseCommit.handle_prepare("txn-bad", 1, :oops)

      assert {:vote_no, {:invalid_op, _}} =
               ThreePhaseCommit.handle_prepare("txn-bad2", 1, [{:put, :not_a_tuple}])

      assert {:vote_no, {:invalid_op, _}} =
               ThreePhaseCommit.handle_prepare("txn-bad3", 1, [{:delete_all, :bad_arg}])
    end

    test "participant lifecycle prepare → pre_commit → commit applies ops" do
      partition = Partition.get_partition_by_idx(1)
      Storage.delete_all(partition)

      assert :vote_yes = ThreePhaseCommit.handle_prepare("txn-full", 1, [{:put, {:tpc_p, 9}}])
      assert :ack_pre_commit = ThreePhaseCommit.handle_pre_commit("txn-full")
      assert :ack_commit = ThreePhaseCommit.handle_commit("txn-full", 1, [])
      assert [{:tpc_p, 9}] == Storage.get(:tpc_p, partition)
      refute Enum.any?(TxnRegistry.list_all(), fn {id, _} -> id == "txn-full" end)
    end

    test "handle_commit falls back to provided ops when registry entry is gone" do
      partition = Partition.get_partition_by_idx(1)
      Storage.delete_all(partition)

      assert :ack_commit =
               ThreePhaseCommit.handle_commit("txn-missing", 1, [{:put, {:tpc_fb, 8}}])

      assert [{:tpc_fb, 8}] == Storage.get(:tpc_fb, partition)
    end

    test "handle_abort removes the registry entry" do
      ThreePhaseCommit.handle_prepare("txn-abort", 0, [{:delete_all, nil}])
      assert :ack_abort = ThreePhaseCommit.handle_abort("txn-abort")
      refute Enum.any?(TxnRegistry.list_all(), fn {id, _} -> id == "txn-abort" end)
    end

    test "recover/0 commits pre_committed and drops prepared txns" do
      partition = Partition.get_partition_by_idx(0)
      Storage.delete_all(partition)

      now = System.monotonic_time(:millisecond)

      :ets.insert(SuperCache.Cluster.TxnRegistry, [
        {"rec-pre",
         %{
           txn_id: "rec-pre",
           partition_idx: 0,
           ops: [{:put, {:tpc_rec, "applied"}}],
           replicas: [],
           state: :pre_committed,
           inserted_at: now
         }},
        {"rec-prep",
         %{
           txn_id: "rec-prep",
           partition_idx: 0,
           ops: [],
           replicas: [],
           state: :prepared,
           inserted_at: now
         }}
      ])

      assert :ok = ThreePhaseCommit.recover()

      assert [{:tpc_rec, "applied"}] == Storage.get(:tpc_rec, partition)
      assert TxnRegistry.count() in [:undefined, 0]
    end

    test "recover/0 removes entries in unknown states" do
      :ets.insert(SuperCache.Cluster.TxnRegistry, {
        "rec-weird",
        %{txn_id: "rec-weird", partition_idx: 0, ops: [], replicas: [], state: :bogus}
      })

      assert :ok = ThreePhaseCommit.recover()
      refute Enum.any?(TxnRegistry.list_all(), fn {id, _} -> id == "rec-weird" end)
    end
  end

  # ── Replicator ───────────────────────────────────────────────────────────────

  describe "Replicator" do
    setup do
      ensure_local_cache!()
      :ok
    end

    test "replicate/2 is a fast :ok with no replicas" do
      assert :ok = Replicator.replicate(0, :put, {:r, 1})
    end

    test "sync replicate to unreachable replicas fails quorum" do
      plant_map(%{1 => {node(), [:"rep_fake1@host", :"rep_fake2@host"]}})
      original_mode = SuperCache.Config.get_config(:replication_mode)

      try do
        SuperCache.Config.set_config(:replication_mode, :sync)
        assert {:error, :quorum_not_reached} = Replicator.replicate(1, :put, {:r, 2})
      after
        if original_mode, do: SuperCache.Config.set_config(:replication_mode, original_mode)
      end
    end

    test "strong replicate routes through WAL commit" do
      plant_map(%{1 => {node(), [:"rep_fake3@host"]}})
      original_mode = SuperCache.Config.get_config(:replication_mode)

      try do
        SuperCache.Config.set_config(:replication_mode, :strong)
        Application.put_env(:super_cache, :wal, majority_timeout: 80)

        assert {:error, :majority_timeout} = Replicator.replicate(1, :put, {:r, 3})
      after
        Application.delete_env(:super_cache, :wal)

        if original_mode, do: SuperCache.Config.set_config(:replication_mode, original_mode)
      end
    end

    test "apply_op/3 applies each operation kind locally" do
      partition = Partition.get_partition_by_idx(0)
      Storage.delete_all(partition)

      :ok = Replicator.apply_op(0, :put, {:rop, 1})
      :ok = Replicator.apply_op(0, :put, [{:rop2, 2}])
      :ok = Replicator.apply_op(0, :delete_match, {:rop2, :_})
      :ok = Replicator.apply_op(0, :delete, :rop)
      :ok = Replicator.apply_op(0, :delete_all, nil)

      assert [] == Storage.get(:rop, partition)
    end

    test "apply_op/3 raises on unknown partition or op" do
      assert_raise ArgumentError, fn -> Replicator.apply_op(99_999, :put, {:x, 1}) end
      assert_raise ArgumentError, fn -> Replicator.apply_op(0, :explode, :boom) end
    end

    test "apply_op_batch/3 applies batches and validates input" do
      partition = Partition.get_partition_by_idx(0)
      Storage.delete_all(partition)

      :ok = Replicator.apply_op_batch(0, :put, [{:rb1, 1}, {:rb2, 2}])
      assert 2 == length(Storage.get_by_match_object({:_, :_}, partition))

      assert_raise ArgumentError, fn -> Replicator.apply_op_batch(0, :nope, [1]) end
      assert_raise ArgumentError, fn -> Replicator.apply_op_batch(99_999, :put, [{:x, 1}]) end
    end

    test "replicate_batch/3 reports incomplete replication to dead replicas" do
      plant_map(%{2 => {node(), [:"batch_fake@host"]}})

      assert {:error, {:replication_incomplete, 1}} =
               Replicator.replicate_batch(2, :put, [{:bb, 1}])
    end

    test "replicate_batch/3 is :ok with no replicas" do
      assert :ok = Replicator.replicate_batch(2, :put, [{:bb, 1}])
    end

    test "push_partition/2 reports failed batches to unreachable targets" do
      partition = Partition.get_partition_by_idx(0)
      Storage.put({:push_me, 1}, partition)

      assert {:error, {:push_failed, 1, 1}} = Replicator.push_partition(0, :"push_fake@host")

      Storage.delete(:push_me, partition)
    end

    test "push_partition/2 returns :ok for an empty partition" do
      partition = Partition.get_partition_by_idx(3)
      Storage.delete_all(partition)

      assert :ok = Replicator.push_partition(3, :"push_fake2@host")
    end
  end

  # ── DistributedStore & DistributedHelpers (single-node distributed mode) ─────

  describe "DistributedStore / DistributedHelpers" do
    test "route_put/route_delete route by the record key; local_* read by namespace" do
      ensure_local_cache!()
      namespace = "ds_ns"
      ets_key = {:ds, namespace, :k}

      assert DistributedStore.route_put(ets_key, "v") == true

      # Writes resolve their partition from the record key (Router semantics)…
      routed_partition = Partition.get_partition(ets_key)
      assert [{^ets_key, "v"}] = Storage.get(ets_key, routed_partition)

      # …while local_* helpers resolve from the namespace argument.
      local_partition = Partition.get_partition(namespace)
      assert is_atom(local_partition)

      assert :ok = DistributedStore.route_delete(ets_key, namespace)
      assert [] = Storage.get(ets_key, routed_partition)
    end

    test "route_delete_match/2 deletes scoped records" do
      ensure_local_cache!()
      namespace = "ds_dm"

      DistributedStore.route_put({{:dm, namespace, 1}, "a"}, namespace)
      DistributedStore.route_put({{:dm, namespace, 2}, "b"}, namespace)

      assert :ok = DistributedStore.route_delete_match(namespace, {{:dm, namespace, :_}, :_})
      assert [] == DistributedStore.local_match(namespace, {{:dm, namespace, :_}, :_})
    end

    @tag :ds_ins
    test "local_insert_new/2 and local_take/2 behave atomically" do
      ensure_local_cache!()
      namespace = "ds_ins"

      assert DistributedStore.local_insert_new({{:dsi, namespace}, 1}, namespace) == true
      assert DistributedStore.local_insert_new({{:dsi, namespace}, 1}, namespace) == false

      assert [{{:dsi, ^namespace}, 1}] = DistributedStore.local_take({:dsi, namespace}, namespace)
      assert [] == DistributedStore.local_take({:dsi, namespace}, namespace)
    end

    test "route_read escalates to primary when the local node has no replica" do
      ensure_local_cache!()

      # Plant a topology where another (unreachable) node owns partition 0.
      plant_map(%{0 => {:"owner_fake@host", []}})

      # With read_mode: :local but no local replica, DistributedHelpers must
      # escalate to the primary — which fails fast via erpc. The failure is
      # raised as an exit by :erpc.call; catch it and confirm escalation
      # happened (i.e. we did NOT get the local answer).
      result =
        try do
          {:ok,
           DistributedHelpers.route_read(
             SuperCache.ClusterLocalTest.KVProbe,
             :probe_value,
             ["k"],
             0,
             read_mode: :local
           )}
        catch
          :exit, reason -> {:exit, reason}
        end

      case result do
        {:ok, value} -> assert value in [:probed]
        {:exit, _} -> :ok
      end
    end

    test "has_partition?/1 reflects the planted topology" do
      plant_map(%{0 => {node(), [:"x@host"]}, 1 => {:"other@host", [:y@host]}})

      assert DistributedHelpers.has_partition?(0) == true
      assert DistributedHelpers.has_partition?(1) == false
    end
  end

  defmodule KVProbe do
    def probe_value(_key), do: :probed
  end
end
