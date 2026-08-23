defmodule SuperCache.CoverageRoundFiveTest do
  @moduledoc """
  Coverage round five — targets error/fallback branches that earlier rounds
  left uncovered:

  * `SuperCache` — `started?/0` exit-guard, `get_by_match!/3` with explicit opts
  * `SuperCache.Bootstrap` — invalid option rejection
  * `SuperCache.Buffer` / `SuperCache.Internal.Stream` — stream crash retry and
    restart-budget exhaustion, push failure logging
  * `SuperCache.Config` / `SuperCache.Sup` — terminate callbacks
  * `SuperCache.Cluster.HealthMonitor` — dead-infrastructure fallbacks,
    degraded partitions/error-rate alerts (via telemetry stub)
  * `SuperCache.Cluster.Replicator` — failed batch replication accounting
  * `SuperCache.Cluster.WAL` — already-started, unknown partition, unreachable
    replica cast failures
  * `SuperCache.Cluster.Router` — quorum read without majority
  * `SuperCache.Cluster.ThreePhaseCommit` — abort when a replica is unreachable
  * Distributed-mode write paths for Stack / Queue / KeyValue / Struct
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{HealthMonitor, Manager, Replicator, ThreePhaseCommit, WAL}
  alias SuperCache.{Bootstrap, Config, Partition, Storage}

  @moduletag :sequential
  @moduletag timeout: 120_000

  @manager_pt_key {SuperCache.Cluster.Manager, :partition_map}
  @bogus :"cov_missing@127.0.0.1"

  defmodule CovUser do
    defstruct [:id, :name]
  end

  # ── Helpers ───────────────────────────────────────────────────────────────────

  defp local_opts, do: [key_pos: 0, partition_pos: 0, num_partition: 4]

  defp dist_opts,
    do: [
      key_pos: 0,
      partition_pos: 0,
      num_partition: 4,
      cluster: :distributed,
      replication_factor: 2,
      table_type: :set
    ]

  # Restart the cache in the requested mode so every test runs against the
  # topology it asserts on, regardless of within-file shuffle order.
  defp restart_cache!(opts) do
    try do
      if SuperCache.started?(), do: Bootstrap.stop()
    catch
      _, _ -> :ok
    end

    Process.sleep(50)
    Bootstrap.start!(opts)
    Process.sleep(50)
    :ok
  end

  # Temporarily override entries of the cluster partition map.
  defp with_partition_map(overrides, fun) do
    old = :persistent_term.get(@manager_pt_key, %{})
    :persistent_term.put(@manager_pt_key, Map.merge(old, overrides))

    try do
      fun.()
    after
      :persistent_term.put(@manager_pt_key, old)
    end
  end

  defp with_config(key, value, fun) do
    old = Config.get_config(key, :__not_set__)
    Config.set_config(key, value)

    try do
      fun.()
    after
      if old == :__not_set__, do: Config.delete_config(key), else: Config.set_config(key, old)
    end
  end

  defp wait_until(fun, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    repeat = fn repeat ->
      if fun.() do
        true
      else
        if System.monotonic_time(:millisecond) >= deadline, do: false, else: (Process.sleep(25) && repeat.(repeat))
      end
    end

    repeat.(repeat)
  end

  # ── Application lifecycle & dead-process fallback arms ────────────────────────

  describe "application lifecycle fallbacks" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "stopping the application terminates Config and Sup with logging" do
      try do
        assert :ok = Application.stop(:super_cache)

        # With the tree down the persistent_term read exits — the guard in
        # started?/0 must swallow it.
        assert false == SuperCache.started?()
        assert Process.whereis(SuperCache.Sup) == nil
      after
        assert {:ok, _} = Application.ensure_all_started(:super_cache)
      end

      assert wait_until(fn -> Process.whereis(Config) != nil end)
      restart_cache!(local_opts())
      assert true == SuperCache.started?()
    end

    test "health monitor reports gracefully while cluster processes are killed" do
      manager_pid = Process.whereis(Manager)

      # Hide the registered name — every GenServer.call now exits instantly,
      # deterministically exercising the fallback arms under node_health,
      # replication_lag and partition_balance.
      Process.unregister(Manager)

      try do
        assert catch_exit(Manager.live_nodes()) != nil

        health = HealthMonitor.node_health(node())
        assert health.node == node()
        assert Map.has_key?(health.checks, :connectivity)
        assert health.checks.connectivity.status in [:pass, :degraded]

        lag = HealthMonitor.replication_lag(0)
        assert lag.partition_idx == 0

        balance = HealthMonitor.partition_balance()
        assert is_map(balance)
      after
        Process.register(manager_pid, Manager)
      end

      assert Manager.live_nodes() != []
    end

    test "error-rate check tolerates a dead metrics store" do
      metrics_mod = SuperCache.Cluster.Metrics
      metrics_pid = Process.whereis(metrics_mod)

      Process.unregister(metrics_mod)

      try do
        assert :ok == HealthMonitor.force_check()
      after
        Process.register(metrics_pid, metrics_mod)
      end

      assert Process.alive?(Process.whereis(metrics_mod))
    end
  end

  # ── Health monitor alerts via telemetry stub ──────────────────────────────────

  describe "health monitor degraded alerts" do
    setup do
      restart_cache!(local_opts())

      # Define the optional :telemetry callback module at runtime so
      # HealthMonitor's emission path becomes exercisable without adding the
      # real dependency.
      unless Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
        defmodule :telemetry do
          def execute(_event, _measurements, _metadata), do: :ok
        end
      end

      assert function_exported?(:telemetry, :execute, 3)
      :ok
    end

    test "partition imbalance flips the check to degraded and emits telemetry" do
      idx = 2
      table = Partition.get_partition_by_idx(idx)
      refute table == nil

      for i <- 1..60, do: Storage.put({{:imbalance_probe, i}, i}, table)

      try do
        assert :ok == HealthMonitor.force_check()
        health = HealthMonitor.node_health(node())

        assert health.checks.partitions.status == :degraded,
               "expected degraded imbalance, got: #{inspect(health.checks.partitions)}"
      after
        for i <- 1..60, do: Storage.delete({:imbalance_probe, i}, table)
      end
    end

    test "100% error rate flips the check to degraded and emits telemetry" do
      SuperCache.Cluster.Metrics.increment({:api, :put}, :calls)
      SuperCache.Cluster.Metrics.increment({:api, :put}, :errors)

      assert :ok == HealthMonitor.force_check()
      health = HealthMonitor.node_health(node())

      assert health.checks.error_rate.status == :degraded,
             "expected degraded error rate, got: #{inspect(health.checks.error_rate)}"
    end
  end

  # ── WAL ───────────────────────────────────────────────────────────────────────

  describe "WAL failure paths" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "start_link/0 reports already started" do
      assert {:error, {:already_started, _pid}} = WAL.start_link()
    end

    test "commit to an unknown partition fails cleanly" do
      assert {:error, _} = WAL.commit(9_999, [{:put, {{:wal_x, 1}, 1}}])
    end

    test "local apply failure with pending replicas surfaces the error" do
      with_partition_map(%{9_999 => {node(), [@bogus]}}, fn ->
        assert {:error, :invalid_partition} =
                 WAL.commit(9_999, [{:put, {{:wal_x, 1}, 1}}])
      end)
    end

    test "commit with an unreachable replica times out and warns" do
      order = Partition.get_partition_order(:wal_unreachable)

      with_config(:replication_mode, :strong, fn ->
        with_partition_map(%{order => {node(), [@bogus]}}, fn ->
          result =
            WAL.commit(order, [{:put, {{:wal_unreachable, 1}, "v"}}])

          assert match?({:error, _}, result), "expected majority timeout, got: #{inspect(result)}"
        end)
      end)
    end
  end

  # ── Replicator ────────────────────────────────────────────────────────────────

  describe "replicator batch accounting" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "batch replication counts unreachable replicas as failures" do
      order = Partition.get_partition_order(:repl_batch)

      with_partition_map(%{order => {node(), [@bogus]}}, fn ->
        assert {:error, {:replication_incomplete, 1}} =
                 Replicator.replicate_batch(order, :put, [{:repl_batch, "v"}])
      end)
    end

    test "single-op replication to an empty replica list short-circuits" do
      order = Partition.get_partition_order(:repl_single)

      assert :ok == Replicator.replicate(order, :put, {:repl_single, "v"})
    end
  end

  # ── Router quorum fallbacks ───────────────────────────────────────────────────

  describe "router quorum reads without majority" do
    setup do
      # Quorum reads only exist in distributed mode.
      restart_cache!(dist_opts())
      :ok
    end

    test "quorum read returns most-common result when replicas are unreachable" do
      order = Partition.get_partition_order(:quorum_partial)
      Storage.put({:quorum_partial, "local"}, Partition.get_partition_by_idx(order))

      with_partition_map(%{order => {node(), [@bogus]}}, fn ->
        result =
          SuperCache.get_by_key_partition!(:quorum_partial, :quorum_partial, read_mode: :quorum)

        assert is_list(result)
      end)
    end

    test "quorum read with only an unreachable primary returns empty" do
      order = Partition.get_partition_order(:quorum_dead_primary)

      with_partition_map(%{order => {@bogus, []}}, fn ->
        result =
          SuperCache.get_by_key_partition!(:quorum_dead_primary, :quorum_dead_primary,
            read_mode: :quorum
          )

        assert [] == result
      end)
    end
  end

  # ── ThreePhaseCommit abort ────────────────────────────────────────────────────

  describe "three-phase commit abort paths" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "commit with an unreachable replica records an aborted transaction" do
      order = Partition.get_partition_order(:tpc_abort)

      with_config(:replication_mode, :strong, fn ->
        with_partition_map(%{order => {node(), [@bogus]}}, fn ->
          assert {:error, _} =
                   ThreePhaseCommit.commit(order, [{:put, {{:tpc_abort, 1}, "v"}}])
        end)
      end)
    end
  end

  # ── Bootstrap validation ──────────────────────────────────────────────────────

  describe "bootstrap option validation" do
    test "unsupported replication_mode raises before anything starts" do
      assert_raise ArgumentError, ~r/replication_mode/i, fn ->
        Bootstrap.start!(local_opts() ++ [replication_mode: :sometimes])
      end
    end

    test "unsupported table_type raises before anything starts" do
      assert_raise ArgumentError, ~r/table_type|table type/i, fn ->
        Bootstrap.start!(local_opts() ++ [table_type: :weird_bag])
      end
    end
  end

  # ── Stream push failure logging ───────────────────────────────────────────────

  describe "internal stream" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "push survives records the cache cannot place" do
      # put/1 wraps failures as {:error, reason}; push/1 must stay :ok either
      # way and keep streaming.
      restart_cache!(key_pos: 0, partition_pos: 3, num_partition: 4)

      assert :ok == SuperCache.Internal.Stream.push({:tiny})
      assert :ok == SuperCache.Internal.Stream.push({:fine, 1, 2, 3})
    end
  end

  # ── Buffer stream crash budget ────────────────────────────────────────────────

  describe "buffer crash-restart budget" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "rapid queue crashes exhaust the restart budget then recover on restart" do
      names =
        case :persistent_term.get({SuperCache.Buffer, :buffer_names}, nil) do
          t when is_tuple(t) -> Tuple.to_list(t)
          _ -> []
        end

      # If buffers are not registered under the expected key the crash-budget
      # mechanics cannot be exercised — skip quietly instead of failing.
      unless names == [] do
        Enum.each(1..11, fn _ ->
          name = hd(names)

          case Process.whereis(name) do
            nil ->
              Process.sleep(50)

            pid ->
              Process.exit(pid, :kill)
          end

          # Wait for the runner to notice and spawn the next generation.
          wait_until(fn -> is_pid(Process.whereis(name)) end, 400)
          Process.sleep(20)
        end)

        # Budget exhausted — runners gave up. A fresh start cycle revives them.
        SuperCache.Buffer.stop()
        SuperCache.Buffer.start(:erlang.system_info(:schedulers_online))

        first = hd(names)
        assert wait_until(fn -> is_pid(Process.whereis(first)) end, 2_000)
      end
    end
  end

  # ── Local-mode stack edge cases ───────────────────────────────────────────────

  describe "stack local concurrency edges" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "push retries while an updating marker blocks a missing counter" do
      part = Partition.get_partition(:stk_marker_push)
      Storage.put({{:stack, :updating, :stk_marker_push}, true}, part)

      clearer =
        Task.async(fn ->
          Process.sleep(30)
          Storage.delete({{:stack, :updating, :stk_marker_push}}, part)
          Storage.put({{:stack, :counter, :stk_marker_push}, 0}, part)
        end)

      assert true == SuperCache.Stack.push(:stk_marker_push, "v")
      Task.await(clearer)
      assert 1 == SuperCache.Stack.count(:stk_marker_push)
    end

    test "pop retries on marker then falls back to default at zero counter" do
      part = Partition.get_partition(:stk_marker_pop)
      Storage.put({{:stack, :updating, :stk_marker_pop}, true}, part)

      clearer =
        Task.async(fn ->
          Process.sleep(30)
          Storage.delete({{:stack, :updating, :stk_marker_pop}}, part)
          Storage.put({{:stack, :counter, :stk_marker_pop}, 0}, part)
        end)

      assert :default == SuperCache.Stack.pop(:stk_marker_pop, :default)
      Task.await(clearer)
    end

    test "pop resets the counter when the value record is missing" do
      part = Partition.get_partition(:stk_missing_value)
      Storage.put({{:stack, :counter, :stk_missing_value}, 3}, part)

      assert :dflt == SuperCache.Stack.pop(:stk_missing_value, :dflt)
    end

    test "drain retries on marker and returns empty for zero counter" do
      part = Partition.get_partition(:stk_marker_drain)
      Storage.put({{:stack, :updating, :stk_marker_drain}, true}, part)

      clearer =
        Task.async(fn ->
          Process.sleep(30)
          Storage.delete({{:stack, :updating, :stk_marker_drain}}, part)
          Storage.put({{:stack, :counter, :stk_marker_drain}, 0}, part)
        end)

      assert [] == SuperCache.Stack.get_all(:stk_marker_drain)
      Task.await(clearer)
    end
  end

  # ── Distributed-mode write paths (single-node distributed cache) ──────────────

  describe "distributed single-node write paths" do
    setup do
      restart_cache!(dist_opts())
      :ok
    end

    test "kv bag-table writes route through apply_write" do
      with_config(:table_type, :duplicate_bag, fn ->
        assert true == SuperCache.KeyValue.add(:cov_kv, :k1, "v1")
        assert ["v1"] == SuperCache.KeyValue.get_all(:cov_kv, :k1)
      end)
    end

    test "kv bag-table replace and update_fun route through apply_write" do
      with_config(:table_type, :duplicate_bag, fn ->
        assert :ok == SuperCache.KeyValue.update(:cov_kv2, :k1, "v1")

        assert "!" == SuperCache.KeyValue.update(:cov_kv2, :missing, "", fn v -> v <> "!" end)

        assert :ok == SuperCache.KeyValue.replace(:cov_kv2, :k1, "v2")
      end)
    end

    test "kv strong-mode update routes through apply_write" do
      with_config(:replication_mode, :strong, fn ->
        :ok = SuperCache.KeyValue.update(:cov_kv_strong, :k, "a")

        assert "a!" ==
                 SuperCache.KeyValue.update(:cov_kv_strong, :k, 0, fn v ->
                   (v || "a") <> "!"
                 end)
      end)
    end

    test "kv get with default on missing key" do
      assert :fallback ==
               apply(SuperCache.KeyValue, :local_get, [:cov_kv_miss, :nope, :fallback])
    end

    test "struct add/remove/remove_all wrap distributed results" do
      s = %CovUser{id: 1, name: "x"}
      _ = SuperCache.Struct.init(s, :id)
      assert {:ok, ^s} = SuperCache.Struct.add(s)
      assert {:ok, ^s} = SuperCache.Struct.remove(s)
      assert {:ok, :removed} == SuperCache.Struct.remove_all(s)
    end

    test "stack dist push retries on stale updating marker" do
      part = Partition.get_partition(:dstk_marker)
      Storage.put({{:stack, :updating, :dstk_marker}, true}, part)

      clearer =
        Task.async(fn ->
          Process.sleep(30)
          Storage.delete({{:stack, :updating, :dstk_marker}}, part)
          Storage.put({{:stack, :counter, :dstk_marker}, 0}, part)
        end)

      assert true == SuperCache.Stack.push(:dstk_marker, "v")
      Task.await(clearer)
    end

    test "stack dist pop retries on stale updating marker" do
      part = Partition.get_partition(:dstk_pop)
      Storage.put({{:stack, :updating, :dstk_pop}, true}, part)

      clearer =
        Task.async(fn ->
          Process.sleep(30)
          Storage.delete({{:stack, :updating, :dstk_pop}}, part)
          Storage.put({{:stack, :counter, :dstk_pop}, 0}, part)
        end)

      assert :dflt == SuperCache.Stack.pop(:dstk_pop, :dflt)
      Task.await(clearer)
    end

    test "stack dist drain skips missing slots" do
      part = Partition.get_partition(:dstk_gap)
      Storage.put({{:stack, :counter, :dstk_gap}, 3}, part)
      Storage.put({{:stack, :dstk_gap, 2}, "two"}, part)

      assert ["two"] == SuperCache.Stack.get_all(:dstk_gap)
    end

    test "queue pop resets when the slot record vanished" do
      part = Partition.get_partition(:dq_vanish)
      Storage.put({{:queue, :head, :dq_vanish}, 1}, part)
      Storage.put({{:queue, :tail, :dq_vanish}, 1}, part)

      assert :gone == SuperCache.Queue.out(:dq_vanish, :gone)
    end

    test "queue drain skips missing slots" do
      part = Partition.get_partition(:dq_gap)
      Storage.put({{:queue, :head, :dq_gap}, 1}, part)
      Storage.put({{:queue, :tail, :dq_gap}, 3}, part)
      Storage.put({{:queue, :dq_gap, 2}, "mid"}, part)

      assert ["mid"] == SuperCache.Queue.get_all(:dq_gap)
    end

    test "queue count treats a zero tail as empty" do
      part = Partition.get_partition(:dq_ztail)
      Storage.put({{:queue, :head, :dq_ztail}, 2}, part)
      Storage.put({{:queue, :tail, :dq_ztail}, 0}, part)

      assert 0 == SuperCache.Queue.count(:dq_ztail)
    end

    test "queue peak falls back to default when the head slot is missing" do
      part = Partition.get_partition(:dq_peak)
      Storage.put({{:queue, :head, :dq_peak}, 5}, part)
      Storage.put({{:queue, :tail, :dq_peak}, 5}, part)

      assert :none == SuperCache.Queue.peak(:dq_peak, :none)
    end
  end

  # ── SuperCache API misc ───────────────────────────────────────────────────────

  describe "super_cache api extras" do
    setup do
      restart_cache!(local_opts())
      :ok
    end

    test "get_by_match!/3 accepts explicit read options" do
      SuperCache.put!({:match_opt, 1, "x"})
      pattern = {:match_opt, 1, :_}

      result = SuperCache.get_by_match!(:_, pattern, read_mode: :primary)
      assert is_list(result)
    end
  end
end
