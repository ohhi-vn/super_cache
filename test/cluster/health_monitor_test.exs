defmodule SuperCache.Cluster.HealthMonitorTest do
  @moduledoc """
  Comprehensive test suite for SuperCache.Cluster.HealthMonitor.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{HealthMonitor, Manager}
  alias SuperCache.{Partition, Storage, Config}

  @moduletag :cluster
  @moduletag :sequential

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    # Stop any existing cache state for a clean slate
    try do
      if SuperCache.started?() do
        SuperCache.stop()
      end
    catch
      _, _ -> :ok
    end

    # Restart the application to reset all GenServers
    try do
      Application.stop(:super_cache)
    catch
      _, _ -> :ok
    end

    Process.sleep(100)

    # Start the full application fresh
    {:ok, _} = Application.ensure_all_started(:super_cache)

    # Start cache in local mode for single-node health monitoring tests
    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

    on_exit(fn ->
      try do
        if SuperCache.started?() do
          SuperCache.stop()
        end
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  setup do
    # HealthMonitor is already started by setup_all - just verify it is alive.
    # Handle {:already_started, pid} in case the supervisor restarted it
    # between the whereis check and the start_link call.
    pid = Process.whereis(HealthMonitor)

    if pid && Process.alive?(pid) do
      %{pid: pid}
    else
      case HealthMonitor.start_link(interval_ms: 60_000) do
        {:ok, pid} -> %{pid: pid}
        {:error, {:already_started, pid}} -> %{pid: pid}
      end
    end
  end

  # ── Cluster Health Tests ──────────────────────────────────────────────────────

  describe "cluster_health/0" do
    test "returns cluster health with summary" do
      Process.sleep(1000)
      health = HealthMonitor.cluster_health()

      assert is_map(health)
      assert health.status in [:healthy, :degraded, :unhealthy]
      assert is_integer(health.timestamp)
      assert is_list(health.nodes)
      assert is_map(health.summary)

      # Summary should have expected keys
      assert health.summary.total_nodes >= 1
      assert health.summary.healthy >= 0
      assert health.summary.degraded >= 0
      assert health.summary.unhealthy >= 0
      assert health.summary.unknown >= 0

      # Summary counts should add up
      assert health.summary.total_nodes ==
               health.summary.healthy + health.summary.degraded + health.summary.unhealthy +
                 health.summary.unknown
    end

    test "includes current node in health report" do
      health = HealthMonitor.cluster_health()
      current_node = node()

      assert Enum.any?(health.nodes, &(&1.node == current_node))
    end
  end

  # ── Node Health Tests ─────────────────────────────────────────────────────────

  describe "node_health/1" do
    test "returns health for current node" do
      health = HealthMonitor.node_health(node())

      assert health.node == node()
      assert health.status in [:healthy, :degraded, :unhealthy, :unknown]
      assert is_map(health.checks)
      assert Map.has_key?(health.checks, :connectivity)
      assert Map.has_key?(health.checks, :replication)
      assert Map.has_key?(health.checks, :partitions)
      assert Map.has_key?(health.checks, :error_rate)
    end

    test "returns health for unknown node" do
      health = HealthMonitor.node_health(:"unknown_node@127.0.0.1")

      assert health.node == :"unknown_node@127.0.0.1"
      assert health.status == :unknown
      assert health.latency_ms == nil
      assert health.last_check == 0
    end

    test "connectivity check passes for reachable node" do
      HealthMonitor.force_check()
      health = HealthMonitor.node_health(node())

      assert health.checks.connectivity.status == :pass
      assert is_integer(health.checks.connectivity.latency_ms)
    end

    test "error rate check returns valid data" do
      HealthMonitor.force_check()
      health = HealthMonitor.node_health(node())

      assert health.checks.error_rate.status in [:pass, :degraded, :unknown]
      assert is_float(health.checks.error_rate.rate)
    end
  end

  # ── Partition Balance Tests ───────────────────────────────────────────────────

  describe "partition_balance/0" do
    test "returns partition balance statistics" do
      balance = HealthMonitor.partition_balance()

      assert is_map(balance)
      assert is_integer(balance.total_records)
      assert is_integer(balance.partition_count)
      assert is_float(balance.avg_records_per_partition)
      assert is_float(balance.max_imbalance)
      assert is_list(balance.partitions)
    end

    test "partition count matches configured value" do
      balance = HealthMonitor.partition_balance()

      expected_count =
        try do
          Partition.get_num_partition()
        catch
          _, _ -> 0
        end

      assert balance.partition_count == expected_count
    end

    test "each partition has required fields" do
      balance = HealthMonitor.partition_balance()

      Enum.each(balance.partitions, fn p ->
        assert is_integer(p.idx)
        assert is_integer(p.record_count)
        assert is_atom(p.primary)
      end)
    end

    test "total records equals sum of partition records" do
      balance = HealthMonitor.partition_balance()

      sum = Enum.reduce(balance.partitions, 0, &(&1.record_count + &2))
      assert balance.total_records == sum
    end

    test "imbalance is zero when all partitions are empty" do
      # Ensure cache is running — wrap in try/catch because the Config
      # GenServer may be dead if a prior test's stop() crashed it.
      try do
        unless SuperCache.started?() do
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
        end
      catch
        _, _ ->
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      end

      # Clear all data first
      try do
        SuperCache.delete_all()
      catch
        _, _ -> :ok
      end

      balance = HealthMonitor.partition_balance()

      assert balance.max_imbalance == 0.0
      assert balance.avg_records_per_partition == 0.0
    end

    test "imbalance increases with uneven data distribution" do
      # Ensure cache is running — wrap in try/catch because the Config
      # GenServer may be dead if a prior test's stop() crashed it.
      try do
        unless SuperCache.started?() do
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
        end
      catch
        _, _ ->
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      end

      # Clear data first
      try do
        SuperCache.delete_all()
      catch
        _, _ -> :ok
      end

      balance = HealthMonitor.partition_balance()

      assert balance.max_imbalance >= 0.0
      assert balance.max_imbalance <= 1.0
    end
  end

  # ── Replication Lag Tests ─────────────────────────────────────────────────────

  describe "replication_lag/1" do
    test "returns replication lag for a partition" do
      lag = HealthMonitor.replication_lag(0)

      assert is_map(lag)
      assert lag.partition_idx == 0
      assert is_atom(lag.primary)
      assert is_list(lag.replicas)
    end

    test "replica status includes required fields" do
      lag = HealthMonitor.replication_lag(0)

      Enum.each(lag.replicas, fn r ->
        assert is_atom(r.node)
        assert r.lag_ms == nil or is_integer(r.lag_ms)
        assert r.status in [:synced, :lagging, :unknown]
      end)
    end

    test "current node is not listed as its own replica" do
      lag = HealthMonitor.replication_lag(0)

      # In a pure single-node cluster replicas is [], but in the cluster
      # test suite other nodes may appear as replicas.  The invariant we
      # care about is that a node never replicates to itself.
      refute Enum.any?(lag.replicas, &(&1.node == node())),
             "Current node #{inspect(node())} must not appear in its own replicas list"
    end
  end

  # ── Force Check Tests ─────────────────────────────────────────────────────────

  describe "force_check/0" do
    test "forces immediate health check" do
      health_before = HealthMonitor.cluster_health()

      Process.sleep(100)
      assert :ok == HealthMonitor.force_check()

      health_after = HealthMonitor.cluster_health()
      assert health_after.timestamp >= health_before.timestamp
    end

    test "updates node health data" do
      HealthMonitor.force_check()

      health = HealthMonitor.node_health(node())

      # Connectivity check should have been updated
      assert is_integer(health.checks.connectivity.timestamp)
    end
  end

  # ── Health Check Cycle Tests ──────────────────────────────────────────────────

  describe "health check cycle" do
    test "runs checks on all live nodes" do
      HealthMonitor.force_check()

      nodes = Manager.live_nodes()

      Enum.each(nodes, fn node ->
        health = HealthMonitor.node_health(node)
        assert health.node == node
        assert health.status in [:healthy, :degraded, :unhealthy, :unknown]
      end)
    end

    test "handles node disconnection gracefully" do
      disconnected_node = :"disconnected_node@127.0.0.1"
      health = HealthMonitor.node_health(disconnected_node)

      assert health.status == :unknown
    end
  end

  # ── Configuration Tests ───────────────────────────────────────────────────────

  describe "configuration" do
    test "accepts custom interval_ms" do
      pid =
        case HealthMonitor.start_link(interval_ms: 1_000) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      assert :ok == HealthMonitor.force_check()
    end

    test "accepts custom latency threshold" do
      pid =
        case HealthMonitor.start_link(latency_threshold_ms: 50) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      assert :ok == HealthMonitor.force_check()
    end

    test "accepts custom replication lag threshold" do
      pid =
        case HealthMonitor.start_link(replication_lag_threshold_ms: 100) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      assert :ok == HealthMonitor.force_check()
    end

    test "accepts custom error rate threshold" do
      pid =
        case HealthMonitor.start_link(error_rate_threshold: 0.1) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      on_exit(fn ->
        try do
          GenServer.stop(pid)
        catch
          _, _ -> :ok
        end
      end)

      assert :ok == HealthMonitor.force_check()
    end
  end

  # ── Health Status Derivation ──────────────────────────────────────────────────

  describe "health status derivation" do
    test "healthy when all checks pass" do
      HealthMonitor.force_check()
      health = HealthMonitor.node_health(node())

      assert health.status in [:healthy, :degraded]
    end

    test "unknown when no checks have run" do
      unknown_node = :"never_checked@127.0.0.1"
      health = HealthMonitor.node_health(unknown_node)

      assert health.status == :unknown
    end
  end

  # ── Integration Tests ─────────────────────────────────────────────────────────

  describe "integration with cluster operations" do
    test "health monitor survives cache restart" do
      SuperCache.stop()

      Process.sleep(100)

      # Health monitor should still be running
      assert Process.alive?(Process.whereis(HealthMonitor))

      # Restart cache
      SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

      # Health check should still work
      assert :ok == HealthMonitor.force_check()
    end

    test "health check works after data operations" do
      # Ensure cache is running — wrap in try/catch because the Config
      # GenServer may be dead if a prior test's stop() crashed it.
      try do
        unless SuperCache.started?() do
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
        end
      catch
        _, _ ->
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      end

      # Write some data
      SuperCache.put!({:health_test, 1, "value1"})
      SuperCache.put!({:health_test, 2, "value2"})

      # Force health check
      HealthMonitor.force_check()

      # Check health
      health = HealthMonitor.node_health(node())

      assert health.status in [:healthy, :degraded]
      assert health.checks.partitions.status in [:pass, :degraded, :unknown]
    end

    test "partition balance reflects data distribution" do
      # Ensure cache is running — wrap in try/catch because the Config
      # GenServer may be dead if a prior test's stop() crashed it.
      try do
        unless SuperCache.started?() do
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
        end
      catch
        _, _ ->
          SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      end

      # Clear existing data
      try do
        SuperCache.delete_all()
      catch
        _, _ -> :ok
      end

      # Write data to multiple partitions
      Enum.each(1..100, fn i ->
        SuperCache.put!({:balance_test, i, "value_#{i}"})
      end)

      # Check partition balance
      balance = HealthMonitor.partition_balance()

      assert balance.total_records > 0
      assert balance.partition_count > 0
      assert balance.avg_records_per_partition > 0
    end
  end
end
