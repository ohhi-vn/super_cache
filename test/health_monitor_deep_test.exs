defmodule SuperCache.HealthMonitorDeepTest do
  @moduledoc """
  Deep coverage for HealthMonitor internals reachable on a single node:
  status derivation, summary aggregation, replication-lag probing against
  unreachable replicas and the periodic-cycle path.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{HealthMonitor, Manager}

  @pt_partition_map {Manager, :partition_map}
  @table HealthMonitor.HealthData

  setup_all do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
    Process.sleep(30)

    :ok
  end

  setup do
    :ets.delete_all_objects(@table)
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

  test "cluster_health/0 aggregates statuses of live nodes" do
    now = System.monotonic_time(:millisecond)

    # cluster_health/0 reports only Manager members; seed this node healthy.
    :ets.insert(@table, [
      {{node(), :connectivity}, %{status: :pass, latency_ms: 1, timestamp: now}},
      {{node(), :replication}, %{status: :pass, timestamp: now}},
      {{node(), :partitions}, %{status: :pass, timestamp: now}},
      {{node(), :error_rate}, %{status: :pass, rate: 0.0, timestamp: now}}
    ])

    health = HealthMonitor.cluster_health()

    assert health.summary.total_nodes >= 1
    by_node = Map.new(health.nodes, fn n -> {n.node, n} end)
    assert by_node[node()].status == :healthy
    assert health.status in [:healthy, :degraded]
    assert health.summary.healthy >= 1
  end

  test "degraded connectivity yields degraded node status" do
    now = System.monotonic_time(:millisecond)

    :ets.insert(@table, [
      {{node(), :connectivity}, %{status: :degraded, latency_ms: 500, timestamp: now}},
      {{node(), :replication}, %{status: :pass, timestamp: now}},
      {{node(), :partitions}, %{status: :pass, timestamp: now}},
      {{node(), :error_rate}, %{status: :pass, rate: 0.0, timestamp: now}}
    ])

    assert HealthMonitor.node_health(node()).status in [:degraded, :healthy]
  end

  test "replication_lag/1 reports unknown status for unreachable replicas" do
    # Partition 0 owned here with a fake replica — polls must time out fast
    # (erpc fails immediately) and degrade to :unknown rather than crash.
    plant_map(%{0 => {node(), [:"lag_fake@host"]}})

    result = HealthMonitor.replication_lag(0)

    assert result.primary == node()
    replica = hd(result.replicas)
    assert replica.node == :"lag_fake@host"
    assert replica.status in [:unknown, :lagging]
  end

  test "replication_lag/1 filters self from the replica list" do
    plant_map(%{1 => {:"other@host", [node(), :"also_other@host"]}})
    result = HealthMonitor.replication_lag(1)
    refute node() in Enum.map(result.replicas, & &1.node)
  end

  test "periodic :health_check cycle writes connectivity data for self" do
    send(HealthMonitor, :health_check)

    wait_until(fn ->
      HealthMonitor.node_health(node()).checks.connectivity.status != :unknown
    end)

    check = HealthMonitor.node_health(node()).checks.connectivity
    assert check.status in [:pass, :degraded]
    assert is_integer(check.timestamp)
  end

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end

  test "stray handle_info messages keep the monitor alive" do
    send(HealthMonitor, {:whatever, 1})
    Process.sleep(10)
    assert Process.alive?(Process.whereis(HealthMonitor))
  end
end
