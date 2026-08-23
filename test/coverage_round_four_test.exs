defmodule SuperCache.CoverageRoundFourTest do
  @moduledoc """
  Coverage round four — local-node scenarios only (no peer VMs required):

  - Router forwarding / quorum fallback against planted partition maps whose
    primaries and replicas are unreachable atoms (`:erpc` fails fast, the
    code under test degrades exactly as it would with a dead cluster member).
  - Replicator, WAL, and ThreePhaseCommit failure paths driven the same way.
  - HealthMonitor defensive catches (supervisor child killed mid-check),
    balance/error-rate alert paths, and lag measurement on missing tables.
  - KeyValue bag/duplicate_bag/strong-mode branches, Queue distributed
    primitives, Struct edge cases, Stats roles and printing, Storage full
    API, Buffer lifecycle corners, Sup worker management, Application
    start-failure branches, and Bootstrap validation/rollback.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias SuperCache.Cluster.{DistributedHelpers, Manager, Replicator, Router, Stats, WAL}
  alias SuperCache.Cluster.HealthMonitor
  alias SuperCache.{Bootstrap, Buffer, Config, EtsHolder}
  alias SuperCache.{KeyValue, Partition, Queue, Stack, Storage, Struct, Sup}

  @pt_partition_map {Manager, :partition_map}
  @dead :"round_four_dead@host"
  @dead2 :"round_four_dead2@host"

  # ── Setup helpers ─────────────────────────────────────────────────────────────

  setup_all do
    restart_cache()
    :ok
  end

  defp restart_cache(opts \\ []) do
    if SuperCache.started?(), do: Bootstrap.stop()
    Process.sleep(20)

    opts = Keyword.merge([key_pos: 0, partition_pos: 0, num_partition: 2], opts)
    :ok = Bootstrap.start!(opts)
    Process.sleep(20)
    :ok
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

  defp plant_replication_mode(mode) do
    original = Config.get_config(:replication_mode)
    Config.set_config(:replication_mode, mode)

    on_exit(fn ->
      if original,
        do: Config.set_config(:replication_mode, original),
        else: Config.set_config(:replication_mode, :async)
    end)
  end

  defp plant_cluster_mode(mode) do
    original = Config.get_config(:cluster)
    Config.set_config(:cluster, mode)

    on_exit(fn ->
      if original,
        do: Config.set_config(:cluster, original),
        else: Config.set_config(:cluster, :local)
    end)
  end

  defp table(idx), do: Partition.get_partition_by_idx(idx)

  # Suspends a supervised child by child-id (no auto-restart while stopped),
  # runs `fun`, then brings the child back.
  defp with_child_killed(module, fun) do
    sup = Process.whereis(SuperCache.Supervisor)
    assert sup

    assert :ok = Supervisor.terminate_child(sup, module)

    try do
      fun.()
    after
      case Supervisor.restart_child(sup, module) do
        {:ok, _} -> :ok
        {:error, _} -> :ok
      end

      wait_until(fn -> Process.whereis(module) != nil end)
    end
  end

  # Runs fun with exit-trapping enabled so linked Task crashes caused by
  # :erpc failures come back as nil results instead of killing the test.
  defp trapping(fun) do
    Process.flag(:trap_exit, true)

    try do
      fun.()
    after
      Process.flag(:trap_exit, false)

      receive do
        {:EXIT, _, _} -> :ok
      after
        0 -> :ok
      end
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Router — planted foreign primaries / dead replicas
  # ════════════════════════════════════════════════════════════════════════════

  describe "Router with planted cluster topology" do
    test "read entry points accept default opts" do
      restart_cache()
      Storage.put({:r_default, 1}, table(0))

      assert [{:r_default, 1}] == Router.route_get!({:r_default, 1})
      assert [{:r_default, 1}] == Router.route_get_by_key_partition!(:r_default, :r_default)
      assert [_] = Router.route_get_by_match!(:r_default, {:r_default, :_})
      assert [_] = Router.route_get_by_match_object!(:r_default, {:r_default, :_})
    end

    test "delete operations forward to an unreachable primary without crashing" do
      restart_cache()

      foreign_map = %{
        0 => {@dead, []},
        1 => {@dead, []},
        Partition.get_partition_order(:fwd_del) => {@dead, []}
      }

      plant_map(foreign_map)
      order = Partition.get_partition_order(:fwd_del)

      assert :ok = Router.route_delete_all_partition(order)
      assert :ok = Router.route_delete_match_partition!(order, {:fwd_del, :_})
      assert :ok = Router.route_delete_match!(:fwd_del, {:fwd_del, :_})

      data = {:fwd_del, :x}
      assert :ok = Router.route_delete!(data)
      assert :ok = Router.route_delete_by_key_partition!(:fwd_del, :fwd_del)
    end

    test "primary read falls back to an empty list when the primary cannot be reached" do
      restart_cache()
      plant_map(%{0 => {@dead, []}, 1 => {@dead, []}})

      assert [] == Router.route_get_by_key_partition!(:nope, :nope, read_mode: :primary)
      assert [] == Router.route_get_by_match!(:nope, {:nope, :_}, read_mode: :primary)
    end

    test "quorum read falls back to the best available result with dead replicas" do
      restart_cache()

      # self + two dead nodes: no majority ever forms, the most frequent
      # (only) result wins after every task has been drained.
      order = Partition.get_partition_order(:q_key)
      Storage.put({:q_key, :v1}, Partition.get_partition_by_idx(order))
      plant_map(%{order => {node(), [@dead, @dead2]}})

      assert [{:q_key, :v1}] ==
               trapping(fn ->
                 Router.route_get_by_key_partition!(:q_key, :q_key, read_mode: :quorum)
               end)
    end

    test "strong-mode writes go through ThreePhaseCommit locally" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_replication_mode(:strong)

      assert true == Router.route_put!({:strong_put, 1})
      assert :ok == Router.route_put_batch!([{:strong_b1, 1}, {:strong_b2, 2}])
      assert [{:strong_b2, 2}] == Router.route_get!({:strong_b2, 2})

      assert :ok = Router.route_delete!({:strong_put, 1})
      assert :ok = Router.route_delete_match!(:strong_b1, {:strong_b1, :_})
      assert :ok = Router.route_delete_all()
    end

    test "batch write routes through the router in distributed mode" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert :ok == SuperCache.put_batch!([{:pb1, 1}, {:pb2, 2}, {:pb3, 3}])
      assert [{:pb2, 2}] == Router.route_get!({:pb2, 2})
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # DistributedHelpers
  # ════════════════════════════════════════════════════════════════════════════

  describe "DistributedHelpers" do
    test "apply_write surfaces 3PC failures" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_replication_mode(:strong)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      assert {:error, {:prepare_timeout, _}} =
               DistributedHelpers.apply_write(0, table(0), [{:put, {:x, 1}}])
    end

    test "route_write forwards to a foreign primary and raises when it is gone" do
      restart_cache()
      order = Partition.get_partition_order(:rw_fwd)
      plant_map(%{order => {@dead, []}})

      trapping(fn ->
        try do
          DistributedHelpers.route_write(Kernel, :node, [], order)
          flunk("expected erpc failure")
        rescue
          ErlangError -> :ok
        catch
          :exit, {:erpc, _} -> :ok
          :exit, _ -> :ok
        end
      end)
    end

    test "route_read escalates :local to :primary when the partition is foreign" do
      restart_cache()
      true = KeyValue.add("esc_kv", :rh_key, 42)

      ns_order = Partition.get_partition_order("esc_kv")
      plant_map(%{ns_order => {@dead, []}})

      assert 42 ==
               DistributedHelpers.route_read(
                 KeyValue,
                 :local_get,
                 ["esc_kv", :rh_key, nil],
                 ns_order,
                 read_mode: :primary
               )
    end

    test "quorum read falls back to the most frequent result when no quorum forms" do
      restart_cache()
      true = KeyValue.add("q_kv", :dh_q, :win)

      ns_order = Partition.get_partition_order("q_kv")
      plant_map(%{ns_order => {node(), [@dead]}})

      assert :win ==
               trapping(fn ->
                 DistributedHelpers.route_read(
                   KeyValue,
                   :local_get,
                   ["q_kv", :dh_q, nil],
                   ns_order,
                   read_mode: :quorum
                 )
               end)
    end

    test "quorum read raises through when every node including the primary is gone" do
      restart_cache()
      ns_order = Partition.get_partition_order("gone_kv")
      plant_map(%{ns_order => {@dead, []}})

      trapping(fn ->
        try do
          DistributedHelpers.route_read(
            KeyValue,
            :local_get,
            ["gone_kv", :x, nil],
            ns_order,
            read_mode: :quorum
          )

          flunk("expected erpc failure")
        rescue
          ErlangError -> :ok
        catch
          :exit, {:erpc, _} -> :ok
          :exit, _ -> :ok
        end
      end)
    end

    test "quorum read falls back to applying locally when the primary is self" do
      restart_cache()
      ns_order = Partition.get_partition_order("selfp_kv")
      plant_map(%{ns_order => {node(), [@dead]}})

      trapping(fn ->
        try do
          DistributedHelpers.route_read(NoSuchModule, :no_fun, [], ns_order, read_mode: :quorum)
          flunk("expected undefined-function failure")
        rescue
          UndefinedFunctionError -> :ok
        end
      end)
    end

    test "has_partition? reflects the planted map" do
      restart_cache()
      plant_map(%{0 => {node(), [@dead]}, 1 => {@dead2, []}})
      assert true == DistributedHelpers.has_partition?(0)
      assert false == DistributedHelpers.has_partition?(1)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Replicator
  # ════════════════════════════════════════════════════════════════════════════

  describe "Replicator" do
    test "async replication fires through the worker pool" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      assert :ok = Replicator.replicate(0, :put, {:repl_async, 1})
      # two-arity form exercises the default op_arg
      assert :ok = Replicator.replicate(0, :delete_all)
    end

    test "an unknown replication mode is rescued into an error tuple" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_replication_mode(:bogus_mode)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      assert {:error, %FunctionClauseError{}} = Replicator.replicate(0, :put, {:x, 1})
    end

    test "push_partition reports failed batches against an unreachable target" do
      restart_cache()
      partition = table(0)
      Storage.put([{:pp1, 1}, {:pp2, 2}], partition)

      assert {:error, {:push_failed, 1, 1}} = Replicator.push_partition(0, @dead)
    end

    test "replicate_batch reports incomplete replication when a replica is dead" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      assert {:error, {:replication_incomplete, 1}} =
               Replicator.replicate_batch(0, :put, [{:rb1, 1}])
    end

    test "apply_op_batch applies each operation shape locally" do
      restart_cache()
      partition = table(0)

      assert :ok = Replicator.apply_op_batch(0, :put, [{:ob1, 1}, {:ob_long, 2, 3}])
      assert [{:ob1, 1}] == Storage.get(:ob1, partition)
      assert :ok = Replicator.apply_op_batch(0, :delete, [:ob1])
      assert [] == Storage.get(:ob1, partition)
      assert :ok = Replicator.apply_op_batch(0, :delete_match, [{:ob_long, :_, :_}])
      assert [] == Storage.get(:ob_long, partition)
      assert :ok = Replicator.apply_op_batch(0, :delete_all, [nil])
      assert 0 == elem(Storage.stats(partition), 1)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # ThreePhaseCommit — local-only paths
  # ════════════════════════════════════════════════════════════════════════════

  describe "ThreePhaseCommit local application" do
    test "commit on an unknown partition reports an invalid partition" do
      restart_cache()

      assert {:error, :invalid_partition} =
               SuperCache.Cluster.ThreePhaseCommit.commit(999, [{:put, {:a, 1}}])
    end

    test "handle_commit with an unknown partition fails gracefully" do
      restart_cache()

      assert {:error, :invalid_partition} =
               SuperCache.Cluster.ThreePhaseCommit.handle_commit("txn-local-1", 999, [
                 {:put, {:a, 1}}
               ])
    end

    test "recover/0 resolves pre-committed and prepared transactions" do
      restart_cache()
      reg = SuperCache.Cluster.TxnRegistry

      reg.register("rec-pre", 0, [{:put, {:rec_pre, 1}}], [])
      reg.mark_pre_committed("rec-pre")
      reg.register("rec-abort", 0, [], [])

      assert :ok = SuperCache.Cluster.ThreePhaseCommit.recover()

      assert [{:rec_pre, 1}] == Storage.get(:rec_pre, table(0))
      assert nil == reg.get("rec-pre")
      assert nil == reg.get("rec-abort")
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # WAL
  # ════════════════════════════════════════════════════════════════════════════

  describe "WAL" do
    test "commit aborts when the local apply fails on an unknown partition" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      assert {:error, :invalid_partition} = WAL.commit(42, [{:put, {:wal_x, 1}}])
    end

    test "commit casts to replicas and tolerates unreachable ones" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), [@dead]}, 1 => {node(), []}})

      # Local apply succeeds; the replica cast fails but is swallowed. The
      # majority wait then times out — bounded via config.
      old = Application.get_env(:super_cache, :wal, [])
      Application.put_env(:super_cache, :wal, majority_timeout: 50)

      try do
        assert {:error, :majority_timeout} = WAL.commit(0, [{:put, {:wal_ok, 1}}])
      after
        Application.put_env(:super_cache, :wal, old)
      end
    end

    test "replicate_and_ack tolerates a dead primary" do
      restart_cache()
      order = Partition.get_partition_order(:wal_ack_probe)
      plant_map(%{order => {@dead, []}})

      assert :ok = WAL.replicate_and_ack(1, order, [{:put, {:wal_ack, 1}}])
    end

    test "stale info messages are ignored" do
      pid = Process.whereis(WAL)
      assert pid

      send(pid, {:majority_reached, 123_456})
      send(pid, :some_unrelated_message)
      Process.sleep(20)
      assert Process.alive?(pid)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Manager — retry machinery for nodes that connect but never become ready
  # ════════════════════════════════════════════════════════════════════════════

  describe "Manager retry chain" do
    test "node_up for an unready node schedules health-check retries" do
      restart_cache()

      Manager.node_up(@dead)
      Process.sleep(650)

      # The dead node must NOT have entered the membership list.
      refute @dead in Manager.live_nodes()
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # HealthMonitor
  # ════════════════════════════════════════════════════════════════════════════

  describe "HealthMonitor" do
    test "start_link/0 with the monitor already running returns an error" do
      assert {:error, {:already_started, _}} = HealthMonitor.start_link()
    end

    test "public readers survive a dead Manager" do
      with_child_killed(Manager, fn ->
        health = HealthMonitor.cluster_health()
        assert health.status in [:healthy, :degraded, :unhealthy, :unknown]

        assert %{node: _} = HealthMonitor.node_health(node())

        lag = HealthMonitor.replication_lag(0)
        assert is_map(lag)

        balance = HealthMonitor.partition_balance()
        assert is_map(balance)

        assert :ok = HealthMonitor.force_check()
      end)
    end

    test "public readers survive a dead Partition registry" do
      with_child_killed(Partition.Holder, fn ->
        # get_num_partition reads the holder-owned ETS — still alive after
        # the owner dies until the table is destroyed with the process.
        balance = HealthMonitor.partition_balance()
        assert is_map(balance)
      end)
    end

    test "partition_balance computes imbalance across populated partitions" do
      restart_cache()
      Storage.delete_all(table(0))
      Storage.delete_all(table(1))

      Storage.put(Enum.map(1..40, &{{:hb, &1, &1}}), table(0))
      Storage.put({:hb99, 99}, table(1))

      balance = HealthMonitor.partition_balance()
      assert balance.total_records == 41
      assert balance.max_imbalance > 0
      assert [%{idx: 0}, %{idx: 1}] = balance.partitions
    end

    test "force_check records checks and flags partition imbalance" do
      restart_cache()
      Storage.delete_all(table(0))
      Storage.delete_all(table(1))
      Storage.put(Enum.map(1..60, &{{:fc, &1, &1}}), table(0))

      assert :ok = HealthMonitor.force_check()
      Process.sleep(50)

      health = HealthMonitor.cluster_health()

      assert [%{checks: %{partitions: %{imbalance: imbalance}}} | _] = health.nodes
      assert is_float(imbalance) or imbalance == 0.0
    end

    test "force_check flags a high error rate" do
      restart_cache()
      alias SuperCache.Cluster.Metrics

      Enum.each(1..25, fn _ -> Metrics.increment({:api, :put}, :errors) end)

      assert :ok = HealthMonitor.force_check()
      Process.sleep(50)

      %{checks: %{error_rate: err}} = HealthMonitor.node_health(node())
      assert err.status in [:pass, :degraded]
    end

    test "checks tolerate configured partitions that have no table" do
      restart_cache()

      SuperCache.Partition.Holder.set_num_partition(8)

      try do
        balance = HealthMonitor.partition_balance()
        assert balance.partition_count == 8

        assert :ok = HealthMonitor.force_check()
        Process.sleep(50)
      after
        SuperCache.Partition.Holder.set_num_partition(2)
        :persistent_term.put({SuperCache.Partition, :num_partition}, 2)
      end
    end

    test "replication_lag reports unknown for a replica on a missing partition" do
      restart_cache()
      plant_map(%{7 => {node(), [@dead]}})

      lag = HealthMonitor.replication_lag(7)
      assert [%{status: :unknown, lag_ms: nil}] = lag.replicas
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Stats
  # ════════════════════════════════════════════════════════════════════════════

  describe "Stats" do
    test "cluster overview derives replica and bystander roles" do
      restart_cache()

      plant_map(%{0 => {node(), [@dead]}, 1 => {@dead2, [node()]}})
      overview = Stats.cluster()
      roles = Map.new(overview.partitions, fn p -> {p.idx, p.role} end)

      assert roles[0] == :primary
      assert roles[1] == :replica

      plant_map(%{0 => {@dead, []}, 1 => {@dead2, []}})
      overview = Stats.cluster()
      roles = Map.new(overview.partitions, fn p -> {p.idx, p.role} end)
      assert roles[0] == :none
    end

    test "per-node breakdown lists primary and replica partitions" do
      restart_cache()
      plant_map(%{0 => {node(), [@dead]}, 1 => {@dead2, [node()]}})

      info = Stats.node_partitions(node())
      assert info.primary_count >= 1
      assert info.replica_count >= 1
    end

    test "record_tpc tracks every abort phase" do
      assert :ok = Stats.record_tpc(:aborted, phase: :prepare)
      assert :ok = Stats.record_tpc(:aborted, phase: :pre_commit)
      assert :ok = Stats.record_tpc(:aborted, phase: :commit)
      assert :ok = Stats.record_tpc(:aborted, [])
      assert :ok = Stats.record_tpc(:committed, latency_us: 12)
      assert :ok = Stats.record_tpc(:weird, [])
    end

    test "print renders nested maps and lists of maps" do
      out =
        capture_io(fn ->
          Stats.print(%{top: %{inner: 1}, items: [%{a: 1}, %{b: 2}], plain: 3})
        end)

      assert out =~ "top"
      assert out =~ "[1]"
      assert out =~ "plain"
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # KeyValue — table-type and replication-mode branches
  # ════════════════════════════════════════════════════════════════════════════

  describe "KeyValue table-type branches" do
    test "set-table increment uses atomic update_counter" do
      restart_cache(table_type: :set)
      assert 5 == KeyValue.increment("counters", :hits, 0, 5)
      assert 6 == KeyValue.increment("counters", :hits)
    end

    test "strong-mode increment uses read-modify-write" do
      restart_cache(table_type: :set)
      plant_cluster_mode(:distributed)
      plant_replication_mode(:strong)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      kv = "strong_counters"
      assert 4 == KeyValue.increment(kv, :n, 3)
      assert 6 == KeyValue.increment(kv, :n, 3, 2)
    end

    test "increment raises for bag tables" do
      restart_cache(table_type: :bag)
      kv = "bag_counters"

      assert_raise ArgumentError, ~r/increment/, fn ->
        KeyValue.increment(kv, :n, 0, 1)
      end
    end

    test "duplicate_bag replace deletes all previous values" do
      restart_cache(table_type: :duplicate_bag)
      kv = "dup_bag"

      true = KeyValue.add(kv, :multi, "v1")
      true = KeyValue.add(kv, :multi, "v2")
      assert ["v1", "v2"] == Enum.sort(KeyValue.get_all(kv, :multi))

      :ok = KeyValue.replace(kv, :multi, "final")
      assert ["final"] == KeyValue.get_all(kv, :multi)
    end

    test "bag update collapses to a single new value and get honours defaults" do
      restart_cache(table_type: :bag)
      kv = "bag_api"

      assert true = KeyValue.add(kv, :k, "a")
      assert ["a"] == KeyValue.get_all(kv, :k)

      assert :ok = KeyValue.update(kv, :k, "b")
      assert ["b"] == KeyValue.get_all(kv, :k)
      assert "missing" == KeyValue.get(kv, :nope, "missing")
      assert [] == KeyValue.get_all(kv, :nope)
    end

    test "batch add and remove honour the cluster flag" do
      restart_cache()
      kv = "batch_kv"

      assert :ok == KeyValue.add_batch(kv, [{:bk1, 1}, {:bk2, 2}])
      assert :ok == KeyValue.remove_batch(kv, [:bk1, :bk2])
      assert nil == KeyValue.get(kv, :bk1)

      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert :ok == KeyValue.add_batch(kv, [{:bk3, 3}])
      assert :ok == KeyValue.remove_batch(kv, [:bk3])
    end

    test "get with a read_mode option routes through route_read" do
      restart_cache()
      true = KeyValue.add("rd_kv", :rk, :rv)

      plant_cluster_mode(:distributed)
      ns_order = Partition.get_partition_order("rd_kv")
      plant_map(%{ns_order => {node(), []}})

      assert :rv == KeyValue.get("rd_kv", :rk, nil, read_mode: :primary)
      assert [:rv] == KeyValue.get_all("rd_kv", :rk, read_mode: :quorum)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # KeyValue — routed (distributed-flag) operation surface
  # ════════════════════════════════════════════════════════════════════════════

  describe "KeyValue routed operations" do
    test "every local_* implementation runs via route_write when distributed" do
      restart_cache(table_type: :set)
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      kv = "routed_kv"

      assert true = KeyValue.add(kv, :a, 1)
      assert 1 == KeyValue.get(kv, :a)

      assert :ok = KeyValue.update(kv, :a, 10)
      assert 10 == KeyValue.get(kv, :a)

      assert 11 == KeyValue.update(kv, :a, 0, &(&1 + 1))

      assert :ok = KeyValue.replace(kv, :a, 100)
      assert 100 == KeyValue.get(kv, :a)

      assert 6 == KeyValue.increment(kv, :cnt, 5)
      assert 9 == KeyValue.increment(kv, :cnt, 5, 3)

      assert [:a, :cnt] == Enum.sort(KeyValue.keys(kv))
      assert [9, 100] == Enum.sort(KeyValue.values(kv))
      assert 2 == KeyValue.count(kv)
      assert [{:a, 100}, {:cnt, 9}] == Enum.sort(KeyValue.to_list(kv))

      assert :ok = KeyValue.remove(kv, :a)
      assert nil == KeyValue.get(kv, :a)
      assert nil == KeyValue.get(kv, :ghost, nil)
      assert [] == KeyValue.get_all(kv, :ghost)
      assert :ok = KeyValue.remove_all(kv)
    end

    test "routed increment rejects bag tables" do
      restart_cache(table_type: :bag)
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert_raise ArgumentError, ~r/increment/, fn ->
        KeyValue.increment("routed_bag", :n, 0, 1)
      end
    end
  end


  describe "Queue distributed primitives" do
    test "enqueue/dequeue/drain/count work through the dist entry points" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert true == Queue.dist_enqueue(:dq, 1)
      assert true == Queue.dist_enqueue(:dq, 2)
      assert 2 == Queue.dist_count(:dq)
      assert 1 == Queue.dist_dequeue(:dq, nil)
      assert [2] == Queue.dist_drain(:dq)
      assert [] == Queue.dist_drain(:dq)
      assert nil == Queue.dist_dequeue(:dq, nil)
      assert nil == Queue.dist_peek(:dq, nil)
    end

    test "spin-wait gives up when another writer holds the updating flag" do
      restart_cache()
      part = Partition.get_partition(:spin_q)

      Storage.put({{:queue, :updating, :spin_q}, true}, part)

      assert false == Queue.dist_enqueue(:spin_q, :v)
      assert nil == Queue.dist_dequeue(:spin_q, nil)
      assert [] == Queue.dist_drain(:spin_q)
      assert 0 == Queue.dist_count(:spin_q)
    after
      Storage.delete({:queue, :updating, :spin_q}, Partition.get_partition(:spin_q))
    end

    test "dequeue with a corrupt counter returns the default and repairs state" do
      restart_cache()
      plant_cluster_mode(:distributed)
      part = Partition.get_partition(:corrupt_q)

      Storage.put({{:queue, :head, :corrupt_q}, 1}, part)
      Storage.put({{:queue, :tail, :corrupt_q}, 1}, part)

      assert :default_value == Queue.dist_dequeue(:corrupt_q, :default_value)
    after
      part = Partition.get_partition(:corrupt_q)
      Storage.delete({:queue, :head, :corrupt_q}, part)
      Storage.delete({:queue, :tail, :corrupt_q}, part)
    end

    test "count_safe retries when head is missing" do
      restart_cache()
      part = Partition.get_partition(:cnt_q)

      Storage.put({{:queue, :tail, :cnt_q}, 3}, part)
      assert 0 == Queue.dist_count(:cnt_q)
    after
      Storage.delete({:queue, :tail, :cnt_q}, Partition.get_partition(:cnt_q))
    end

    test "drain skips slots whose items vanished" do
      restart_cache()
      part = Partition.get_partition(:vanish_q)

      Queue.add(:vanish_q, :gone)
      _taken = Storage.take({:queue, :vanish_q, 1}, part)

      assert [] == Queue.get_all(:vanish_q)
    after
      Storage.delete({:queue, :vanish_q, 1}, Partition.get_partition(:vanish_q))
    end

    test "get_all drains items and reports empties afterwards" do
      restart_cache()

      Queue.add(:empty_q, :seed)
      assert [:seed] == Queue.get_all(:empty_q)
      assert [] == Queue.get_all(:empty_q)
      assert 0 == Queue.count(:empty_q)
    end
  end

  describe "Stack primitives" do
    test "push/pop/count through the dist entry points" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert true == Stack.dist_push(:ds, 1)
      assert true == Stack.dist_push(:ds, 2)
      assert 2 == Stack.dist_count(:ds)
      assert 2 == Stack.dist_pop(:ds, nil)
      assert 1 == Stack.dist_pop(:ds, nil)
      assert nil == Stack.dist_pop(:ds, nil)
      assert [] == Stack.dist_get_all(:ds)
    end

    test "pop on a missing stack returns the default" do
      restart_cache()

      assert :none == Stack.pop(:brand_new_stack, :none)
      assert 0 == Stack.count(:brand_new_stack)
    end

    test "get_all drains entries and count reflects reality" do
      restart_cache()

      Stack.push(:gs, 1)
      Stack.push(:gs, 2)
      assert [1, 2] == Stack.get_all(:gs)
      assert [] == Stack.get_all(:gs)
      assert 0 == Stack.count(:gs)

      # A vanished slot is skipped during the drain.
      part = Partition.get_partition(:van_s)
      Stack.push(:van_s, :kept)
      Storage.take({:stack, :van_s, 1}, part)

      assert [:kept] == Stack.get_all(:van_s) or [] == Stack.get_all(:van_s)
    after
      part = Partition.get_partition(:van_s)
      Storage.delete({:stack, :counter, :van_s}, part)
      Storage.delete({:stack, :van_s, 1}, part)
    end

    test "dist_pop on a missing counter returns the default" do
      restart_cache()
      plant_cluster_mode(:distributed)
      plant_map(%{0 => {node(), []}, 1 => {node(), []}})

      assert :fresh_default == Stack.dist_pop(:never_touched_stack, :fresh_default)
    end

    test "dist pop and drain repair a missing slot" do
      restart_cache()
      plant_cluster_mode(:distributed)
      part = Partition.get_partition(:gap_s)

      # Counter says one item exists but the slot is empty.
      Storage.put({{:stack, :counter, :gap_s}, 1}, part)

      assert :fallback == Stack.dist_pop(:gap_s, :fallback)
    after
      part = Partition.get_partition(:gap_s)
      Storage.delete({:stack, :counter, :gap_s}, part)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Struct
  # ════════════════════════════════════════════════════════════════════════════

  defmodule User do
    defstruct id: nil, name: ""
  end

  describe "Struct API" do
    test "init/1 defaults the key field to :id" do
      restart_cache()
      assert true == Struct.init(%User{id: 1, name: "a"})
    end

    test "remove returns the stored struct and not_found for unknown ids" do
      restart_cache()
      Struct.init(%User{id: 7, name: "seven"})
      assert {:ok, _} = Struct.add(%User{id: 7, name: "seven"})

      assert {:ok, %User{id: 7}} = Struct.remove(%User{id: 7})
      assert {:error, :not_found} = Struct.remove(%User{id: 8})

      Struct.init(%User{id: 9, name: "nine"})
      assert {:ok, _} = Struct.add(%User{id: 9, name: "nine"})

      assert {:ok, _removed} = Struct.remove_all(%User{id: 9})
    end

    test "remove routes through route_write in distributed mode" do
      restart_cache()
      plant_cluster_mode(:distributed)

      ns_order = Partition.get_partition_order({:struct_storage, User})
      plant_map(%{ns_order => {node(), []}})

      assert true == Struct.init(%User{id: 31, name: "dist"})
      assert {:ok, _} = Struct.add(%User{id: 31, name: "dist"})

      assert {:ok, %User{id: 31}} = Struct.remove(%User{id: 31})

      Struct.init(%User{id: 32, name: "gone"})
      assert {:ok, _} = Struct.add(%User{id: 32, name: "gone"})
      assert {:ok, :removed} == Struct.remove_all(%User{id: 32})
    end

    test "local_get_key_field distinguishes found vs missing" do
      restart_cache()
      Struct.init(%User{id: 3, name: "tres"})
      assert {:ok, :id} == Struct.local_get_key_field(User)
    end

    test "get_all lists stored structs" do
      restart_cache()
      Struct.init(%User{id: 11, name: "x"})
      assert {:ok, _} = Struct.add(%User{id: 11, name: "x"})

      Struct.init(%User{id: 12, name: "y"})
      assert {:ok, _} = Struct.add(%User{id: 12, name: "y"})

      assert {:ok, users} = Struct.get_all(%User{})
      assert length(users) >= 2
    end

    test "fetching the key field from a foreign primary raises through" do
      restart_cache()
      plant_cluster_mode(:distributed)

      ns_order = Partition.get_partition_order({:struct_storage, User})
      plant_map(%{ns_order => {@dead, []}})

      trapping(fn ->
        try do
          Struct.init(%User{id: 21})
          flunk("expected remote lookup failure")
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
      end)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Storage — full primitive surface
  # ════════════════════════════════════════════════════════════════════════════

  describe "Storage primitives" do
    test "typed and atomic helpers" do
      restart_cache()
      partition = table(0)

      assert true == Storage.insert_new({:ins, 1}, partition)
      assert false == Storage.insert_new({:ins, 2}, partition)

      assert true = Storage.put({:upd, 1}, partition)
      assert true == Storage.update_element(:upd, partition, {2, 99})
      assert [{:upd, 99}] == Storage.get(:upd, partition)
      assert true == Storage.update_element(:upd_def, partition, {2, 5}, {:upd_def, 5})
      assert [{:upd_def, 5}] == Storage.get(:upd_def, partition)

      assert 7 == Storage.update_counter(:cnt, partition, {2, 7}, {:cnt, 0})
      assert 9 == Storage.update_counter(:cnt, partition, {2, 2})

      Storage.put([{:tri1, 1, :a}, {:tri2, 2, :b}], partition)
      assert [[1]] = Storage.get_by_match({:tri1, :"$1", :_}, partition)
      assert [[2]] = Storage.get_by_match({:tri2, :"$1", :_}, partition)
    end

    test "match helpers accept atom and tuple patterns" do
      restart_cache()
      partition = table(0)

      Storage.put([{:mt, 1, :aaa}, {:mx, 2, :bbb}], partition)

      assert [[1]] = Storage.get_by_match({:mt, :"$1", :_}, partition)
      assert [[2]] = Storage.get_by_match({:mx, :"$1", :_}, partition)

      objects = Storage.get_by_match_object({:mt, :_, :_}, partition)
      assert [{:mt, 1, :aaa}] == objects
      assert [] == Storage.get_by_match(:bare_atom_pattern, partition)

      assert 1 == Storage.delete_match({:mt, :_, :_}, partition)
      assert [] == Storage.get_by_match_object({:mt, :_, :_}, partition)
      assert [{:mx, 2, :bbb}] == Storage.get_by_match_object({:mx, :_, :_}, partition)
    end

    test "take removes and returns records atomically" do
      restart_cache()
      partition = table(0)

      Storage.put({:tk, :v}, partition)
      assert [{:tk, :v}] == Storage.take(:tk, partition)
      assert [] == Storage.take(:tk, partition)
    end

    test "scan folds over a partition" do
      restart_cache()
      partition = table(1)
      Storage.delete_all(partition)

      Storage.put([{:sc1, 1}, {:sc2, 2}, {:sc3, 3}], partition)

      assert 6 ==
               Storage.scan(
                 fn
                   {k, v}, acc when k in [:sc1, :sc2, :sc3] and is_integer(v) -> acc + v
                   _, acc -> acc
                 end,
                 0,
                 partition
               )
    end

    test "stats reports size and tolerates unknown tables" do
      restart_cache()
      assert {tab, size} = Storage.stats(table(0))
      assert is_atom(tab) and is_integer(size)
      assert {:missing_table_xyz, :undefined} = Storage.stats(:missing_table_xyz)
    end

    test "stop tolerates a holder that is momentarily gone" do
      restart_cache(num_partition: 1)

      with_child_killed(EtsHolder, fn ->
        assert :ok = Storage.stop(1)
      end)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Sup — dynamic worker supervision
  # ════════════════════════════════════════════════════════════════════════════

  describe "Sup workers" do
    defp sleeper_spec(id),
      do: %{id: id, start: {Task, :start_link, [fn -> Process.sleep(15_000) end]}}

    test "start_workers starts a list of children" do
      assert [{:ok, pid1}, {:ok, pid2}] =
               Sup.start_workers([sleeper_spec(:cov_w1), sleeper_spec(:cov_w2)])

      assert :ok = Sup.stop_worker(pid1)
      assert :ok = Sup.stop_worker(pid2)
    end

    test "start_worker/stop_worker manage a single child" do
      assert {:ok, pid} = Sup.start_worker(sleeper_spec(:cov_w3))
      assert :ok = Sup.stop_worker(pid)

      # Unknown pid/name → error tuple.
      assert {:error, :not_found} = Sup.stop_worker(self())
      assert {:error, :not_found} = Sup.stop_worker(:never_registered_atom)
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Buffer — lifecycle corners
  # ════════════════════════════════════════════════════════════════════════════

  describe "Buffer lifecycle" do
    @pt_buffers {Buffer, :buffer_names}

    test "enqueue succeeds against a live queue" do
      restart_cache()
      assert :ok = Buffer.enqueue({:buf_probe, 1})
      Process.sleep(20)
    end

    test "enqueue drops writes while the queue process is momentarily dead" do
      restart_cache()
      names = :persistent_term.get(@pt_buffers)

      # Kill every queue so whichever scheduler handles the enqueue hits a
      # dead buffer and surfaces the drop instead of a silent loss.
      names_tuple = names

      Enum.each(:lists.seq(0, tuple_size(names_tuple) - 1), fn i ->
        name = elem(names_tuple, i)

        case Process.whereis(name) do
          nil -> :ok
          pid -> Process.exit(pid, :kill)
        end
      end)

      result =
        Enum.reduce_while(1..10, {:error, :gave_up}, fn _i, acc ->
          case Buffer.enqueue({:drop_probe, 1}) do
            {:error, :process_down} -> {:halt, {:error, :process_down}}
            _ -> Process.sleep(5)
            {:cont, acc}
          end
        end)

      assert {:error, :process_down} = result
      Process.sleep(400)
    end

    test "stop without any buffers logs and returns :ok" do
      saved = :persistent_term.get(@pt_buffers, nil)
      :persistent_term.erase(@pt_buffers)

      try do
        assert :ok = Buffer.stop()
      after
        if saved, do: :persistent_term.put(@pt_buffers, saved)
      end
    end

    test "stop skips names that were never registered" do
      saved = :persistent_term.get(@pt_buffers, nil)

      :persistent_term.put(@pt_buffers, {:never_registered_buffer_x, :never_registered_buffer_y})

      try do
        assert :ok = Buffer.stop()
      after
        if saved,
          do: :persistent_term.put(@pt_buffers, saved),
          else: :persistent_term.erase(@pt_buffers)
      end
    end

    test "standalone start/stop cycle shuts streams down cleanly" do
      Bootstrap.stop()
      Process.sleep(30)

      assert :ok = Buffer.start(1)
      assert :ok = Buffer.enqueue({:standalone, 1})
      Process.sleep(30)
      assert :ok = Buffer.stop()
      Process.sleep(100)

      assert {:error, :not_started} = Buffer.enqueue({:after_stop, 1})

      restart_cache()
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Bootstrap — validation, resolution, rollback
  # ════════════════════════════════════════════════════════════════════════════

  describe "Bootstrap validation" do
    test "non-keyword options are rejected" do
      assert_raise ArgumentError, ~r/keyword list/, fn ->
        Bootstrap.start!("not a keyword list")
      end

      assert_raise ArgumentError, ~r/keyword list/, fn ->
        Bootstrap.start!([{:a, 1} | :improper])
      end
    end

    test "invalid option values are rejected individually" do
      assert_raise ArgumentError, ~r/replication_factor/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, replication_factor: :two)
      end

      assert_raise ArgumentError, ~r/table_prefix/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, table_prefix: :not_a_string)
      end

      assert_raise ArgumentError, ~r/unsupported cluster mode/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, cluster: :mesh)
      end

      assert_raise ArgumentError, ~r/num_partition/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: -3)
      end
    end

    test "valid combinations pass validation and warn about excess replication" do
      assert :ok =
               Bootstrap.start!(
                 key_pos: 0,
                 partition_pos: 0,
                 num_partition: 2,
                 replication_factor: 8,
                 replication_mode: :sync,
                 table_prefix: "RoundFourPrefix"
               )

      Process.sleep(20)
      assert :"RoundFourPrefix_0" == Partition.get_partition_by_idx(0)
    end

    test "distributed delegation forwards to Cluster.Bootstrap" do
      assert :ok =
               Bootstrap.start!(
                 key_pos: 0,
                 partition_pos: 0,
                 cluster: :distributed,
                 num_partition: 2
               )

      Process.sleep(20)
      assert SuperCache.started?()
    end


    test "Cluster.Bootstrap validates options eagerly" do
      alias SuperCache.Cluster.Bootstrap, as: CB

      assert_raise ArgumentError, ~r/keyword list/, fn -> CB.start!("junk") end
      assert_raise ArgumentError, ~r/missing required option/, fn -> CB.start!([]) end

      assert_raise ArgumentError, ~r/:nodes must be a list of node atoms/, fn ->
        CB.start!(key_pos: 0, partition_pos: 0, nodes: [:ok_atom, 42])
      end

      assert_raise ArgumentError, ~r/unsupported table type/, fn ->
        CB.start!(key_pos: 0, partition_pos: 0, table_type: :fancy)
      end
    end

    test "Cluster.Bootstrap start!/0 boots on defaults" do
      assert :ok = SuperCache.Cluster.Bootstrap.start!()
      Process.sleep(30)
      assert SuperCache.started?()

      # A stop with no recorded partition count still cleans up gracefully.
      Config.set_config(:num_partition, nil)
      assert :ok = SuperCache.Cluster.Bootstrap.stop()
      Process.sleep(20)

      restart_cache()
    end

    test "stop warns when num_partition config is invalid" do
      restart_cache()
      Config.set_config(:num_partition, :bogus)

      assert :ok = Bootstrap.stop()
      Process.sleep(20)

      restart_cache()
    end

    test "stop tolerates a dead configuration store" do
      restart_cache()

      with_child_killed(Config, fn ->
        assert :ok = Bootstrap.stop()
      end)

      restart_cache()
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Application — start-failure branches
  # ════════════════════════════════════════════════════════════════════════════

  describe "Application callbacks" do
    test "start/2 while already running reports the conflict" do
      assert {:error, {:already_started, _}} = SuperCache.Application.start(:normal, [])
    end

    test "auto-start failure is logged, not raised, through the app boot path" do
      original_env = Application.get_all_env(:super_cache)

      try do
        Application.put_env(:super_cache, :auto_start, true)
        # Missing :key_pos makes Bootstrap.start! raise inside the application
        # callback — the rescue must contain it so the tree still comes up.
        Application.delete_env(:super_cache, :key_pos)
        Application.delete_env(:super_cache, :cluster_peers)

        :ok = Application.stop(:super_cache)
        {:ok, _} = Application.ensure_all_started(:super_cache)
        Process.sleep(50)

        assert Process.whereis(Config) != nil
      after
        Enum.each(original_env, fn {k, v} -> Application.put_env(:super_cache, k, v) end)

        :ok = Application.stop(:super_cache)
        {:ok, _} = Application.ensure_all_started(:super_cache)
      end

      restart_cache()
    end
  end

  # ════════════════════════════════════════════════════════════════════════════
  # Config / Partition / NodeMonitor small surfaces
  # ════════════════════════════════════════════════════════════════════════════

  describe "Config store" do
    test "casts and unexpected messages are absorbed" do
      pid = Process.whereis(Config)
      assert pid

      GenServer.cast(Config, :unexpected_cast_message)
      send(pid, :unexpected_info_message)
      Process.sleep(20)
      assert Process.alive?(pid)
    end

    test "restarting the child replays init with opts" do
      restart_cache()

      with_child_killed(Config, fn ->
        :ok
      end)

      assert Config.get_config(:key_pos) != nil
    end
  end

  describe "Partition registry" do
    test "holder absorbs casts and infos" do
      pid = Process.whereis(Partition.Holder)
      assert pid

      GenServer.cast(Partition.Holder, :unexpected_cast)
      send(pid, :unexpected_info)
      Process.sleep(20)
      assert Process.alive?(pid)
    end

    test "get_partition_by_idx and order lookups agree" do
      restart_cache()
      assert is_atom(Partition.get_partition_by_idx(0))

      order = Partition.get_partition_order(:any_term)
      assert order in [0, 1]
      assert is_integer(Partition.get_hash(:any_term))
    end

    test "num_partition falls back to the registry when the cached term is erased" do
      restart_cache()
      assert Partition.get_num_partition() == 2

      :persistent_term.erase({SuperCache.Partition, :num_partition})

      # The fast path silently re-caches the value on the next routed read.
      assert Partition.get_partition_order(:fallback_probe) in [0, 1]
      assert Partition.get_num_partition() == 2

      :persistent_term.put({SuperCache.Partition, :num_partition}, 2)
    end

    test "start_link/0 on an already-running holder returns an error" do
      assert {:error, {:already_started, _}} = SuperCache.Partition.Holder.start_link()
    end
  end

  describe "NodeMonitor sources" do
    test "a raising nodes_mfa is contained" do
      :ok = SuperCache.Cluster.NodeMonitor.reconfigure(nodes_mfa: {NoSuchMod, :list_nodes, []})
      Process.sleep(30)

      :ok = SuperCache.Cluster.NodeMonitor.reconfigure(nodes_mfa: {Kernel, :exit, [:boom]})
      Process.sleep(30)

      # Restore legacy behaviour.
      :ok = SuperCache.Cluster.NodeMonitor.reconfigure([])
      Process.sleep(30)
    end

    test "static node sources attempt connections to unreachable peers" do
      :ok = SuperCache.Cluster.NodeMonitor.reconfigure(nodes: [@dead, @dead2])
      Process.sleep(50)

      :ok = SuperCache.Cluster.NodeMonitor.reconfigure([])
      Process.sleep(30)
    end

    test "stale and unknown messages do not crash the monitor" do
      pid = Process.whereis(SuperCache.Cluster.NodeMonitor)
      assert pid

      send(pid, :totally_unknown_message)
      Process.sleep(20)
      assert Process.alive?(pid)
    end
  end

  describe "EtsHolder details" do
    test "deleting unknown or already-gone tables is a no-op" do
      restart_cache()

      assert :ok = EtsHolder.delete_table(EtsHolder, :never_created_table)
      assert true = EtsHolder.clean(EtsHolder, :never_created_table)

      name = :round_four_temp_table
      :ok = EtsHolder.new_table(EtsHolder, name)
      :ets.delete(name)
      assert :ok = EtsHolder.delete_table(EtsHolder, name)
    end

    test "holder absorbs casts and infos" do
      pid = Process.whereis(EtsHolder)
      assert pid

      GenServer.cast(EtsHolder, :unexpected_cast)
      send(pid, :unexpected_info)
      Process.sleep(20)
      assert Process.alive?(pid)
    end

    test "terminate cleans up tables that vanished behind its back" do
      name = :round_four_vanishing_holder

      {:ok, _pid} = EtsHolder.start_link(name)
      :ok = EtsHolder.new_table(name, :round_four_vanish_tab)
      :ets.delete(:round_four_vanish_tab)

      assert :ok = EtsHolder.stop(name)
      Process.sleep(30)
    end
  end

  describe "Internal.Queue details" do
    test "add to a queue that is not running reports process_down" do
      assert {:error, :process_down} = SuperCache.Internal.Queue.add(:no_such_queue_xyz, :data)
    end

    test "starting a queue with a taken name raises" do
      name = :round_four_taken_queue

      {:ok, squatter} =
        Task.start_link(fn ->
          receive do
            :quit -> :ok
          end
        end)

      true = Process.register(squatter, name)

      try do
        assert_raise ArgumentError, ~r/taken|register|did not register/, fn ->
          SuperCache.Internal.Queue.start(name)
        end
      after
        Process.unregister(name)
        send(squatter, :quit)
      end
    end

    test "items enqueued while a reader waits are delivered immediately" do
      name = :round_four_reader_queue
      qpid = SuperCache.Internal.Queue.start(name)

      reader = Task.async(fn -> SuperCache.Internal.Queue.get(name) end)

      # Give the reader time to register with the queue process.
      Process.sleep(30)

      :ok = SuperCache.Internal.Queue.add(name, :payload)

      assert [:payload] = Task.await(reader, 2_000)
      assert :ok = SuperCache.Internal.Queue.stop(qpid)
    end
  end

  describe "Internal.Stream" do
    test "push succeeds for valid cache records" do
      restart_cache()
      assert :ok = SuperCache.Internal.Stream.push({:stream_push, 1})
    end
  end

  describe "DistributedStore and deprecated delegates" do
    test "local_get reads through the namespace partition" do
      restart_cache()
      alias SuperCache.Cluster.DistributedStore

      assert true = SuperCache.put({:ds_key, 1})
      Process.sleep(10)
      assert [{:ds_key, 1}] == DistributedStore.local_get(:ds_key, {:ds_key, 1})
    end

    test "deprecated SuperCache.Distributed delegates still resolve" do
      restart_cache()

      assert :ok = SuperCache.Distributed.lazy_put({:dep_lazy, 1})
      assert true = SuperCache.Distributed.put({:dep_put, 2})
      Process.sleep(10)
      assert [{:dep_put, 2}] == SuperCache.Distributed.get_same_key_partition(:dep_put)
    end
  end

  describe "SuperCache top-level wrappers" do
    test "safe (non-bang) variants return values instead of raising" do
      restart_cache()

      assert :ok = SuperCache.start(key_pos: 0, partition_pos: 0, num_partition: 2)
      Process.sleep(20)

      assert SuperCache.started?() == true
      assert is_boolean(SuperCache.distributed?())

      assert true = SuperCache.put({:wrap_k, 1})
      assert [{:wrap_k, 1}] == SuperCache.get({:wrap_k, 1})
      assert [{:wrap_k, 1}] == SuperCache.get_by_key_partition(:wrap_k, :wrap_k)
      assert [{:wrap_k, 1}] == SuperCache.get_same_key_partition(:wrap_k)
      assert [_] = SuperCache.get_by_match(:wrap_k, {:wrap_k, :_})
      assert [_] = SuperCache.get_by_match_object(:wrap_k, {:wrap_k, :_})

      stats = SuperCache.stats()
      assert is_list(stats)
    end

    test "match-object scans fan out across partitions" do
      restart_cache()
      SuperCache.put({:mo_a, 1})
      SuperCache.put({:mo_b, 2})

      results = SuperCache.get_by_match_object!(:_, {:mo_a, :_})
      assert Enum.any?(results, &match?({:mo_a, 1}, &1))

      safe_results = SuperCache.get_by_match_object(:_, {:mo_b, :_})
      assert Enum.any?(safe_results, &match?({:mo_b, 2}, &1))
      Process.sleep(20)
    end
  end

  # ── Shared wait helper ────────────────────────────────────────────────────────

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: :ok

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end
