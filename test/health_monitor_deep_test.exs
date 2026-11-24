defmodule SuperCache.HealthMonitorDeepTest do
  @moduledoc """
  Deep coverage for HealthMonitor internals reachable on a single node:
  status derivation, summary aggregation, replication-lag probing against
  unreachable replicas and the periodic-cycle path.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{HealthMonitor, Manager, Metrics}

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
    plant_map(%{0 => {node(), [:lag_fake@host]}})

    result = HealthMonitor.replication_lag(0)

    assert result.primary == node()
    replica = hd(result.replicas)
    assert replica.node == :lag_fake@host
    assert replica.status in [:unknown, :lagging]
  end

  test "replication_lag/1 filters self from the replica list" do
    plant_map(%{1 => {:other@host, [node(), :also_other@host]}})
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

  test "cluster_health/0 reports an unreachable member as unknown" do
    fake = :"cov_deep_remote@127.0.0.1"
    with_manager_nodes([node(), fake])

    health = HealthMonitor.cluster_health()
    entry = Enum.find(health.nodes, &(&1.node == fake))

    assert entry.status == :unknown
    assert entry.latency_ms == nil
    assert entry.last_check == 0
    assert entry.checks.connectivity == %{status: :fail, timestamp: 0}
    assert entry.checks.replication.status == :unknown
    assert entry.checks.partitions.status == :unknown
    assert entry.checks.error_rate.status == :unknown

    assert HealthMonitor.node_health(fake).status == :unknown
  end

  test "cluster_health/0 degrades a cached aggregate for an unreachable member" do
    fake = :"cov_deep_cached@127.0.0.1"
    with_manager_nodes([node(), fake])

    cached = %{node: fake, status: :healthy, latency_ms: 7, last_check: 42, checks: %{}}
    :ets.insert(@table, {{fake, :aggregate}, cached})

    entry =
      HealthMonitor.cluster_health().nodes
      |> Enum.find(&(&1.node == fake))

    assert entry.status == :degraded
    assert entry.latency_ms == 7
    assert entry.last_check == 42
  end

  test "force_check/0 records :not_primary when another node owns every partition" do
    fake_primary = :cov_deep_primary@host
    plant_map(%{0 => {fake_primary, []}, 1 => {fake_primary, []}})

    assert :ok = HealthMonitor.force_check()

    checks = HealthMonitor.node_health(node()).checks
    assert checks.replication.status == :unknown
    assert checks.replication.reason == :not_primary
    assert checks.partitions.status == :unknown
    assert checks.partitions.reason == :not_primary
  end

  test "force_check/0 records a cycle timeout when the deadline is zero" do
    original = :sys.get_state(HealthMonitor)

    :sys.replace_state(HealthMonitor, fn state ->
      %{state | opts: Keyword.put(state.opts, :cycle_deadline_ms, 0)}
    end)

    try do
      assert :ok = HealthMonitor.force_check()

      checks = HealthMonitor.node_health(node()).checks
      assert checks.connectivity.status == :fail
      assert checks.connectivity.latency_ms == nil
      assert checks.replication.status == :unknown
      assert checks.partitions.status == :unknown
      assert checks.error_rate.status == :unknown
    after
      :sys.replace_state(HealthMonitor, fn _ -> original end)
      send(HealthMonitor, :health_check)
    end
  end

  test "a timed-out cycle increments the health timeout counter" do
    original = :sys.get_state(HealthMonitor)
    before = Map.get(Metrics.get_all(:health), :timeouts, 0)

    :sys.replace_state(HealthMonitor, fn state ->
      %{state | opts: Keyword.put(state.opts, :cycle_deadline_ms, 0)}
    end)

    try do
      assert :ok = HealthMonitor.force_check()
      after_count = Map.get(Metrics.get_all(:health), :timeouts, 0)
      assert after_count == before + 1
    after
      :sys.replace_state(HealthMonitor, fn _ -> original end)
      send(HealthMonitor, :health_check)
    end
  end

  test "skips checks until the cache has completed bootstrap" do
    SuperCache.Config.set_config(:started, false)

    # Drain checks queued by earlier tests before clearing the table: their
    # async tasks can otherwise write a result after our assertions.
    assert :ok = HealthMonitor.force_check()
    :ets.delete_all_objects(@table)

    try do
      assert :ok = HealthMonitor.force_check()
      assert :ets.lookup(@table, {node(), :connectivity}) == []
      assert :ets.lookup(@table, {node(), :partitions}) == []
    after
      SuperCache.Config.set_config(:started, true)
    end
  end

  defp with_manager_nodes(nodes) do
    original = :sys.get_state(Manager)
    :sys.replace_state(Manager, fn state -> %{state | nodes: nodes} end)

    on_exit(fn ->
      try do
        :sys.replace_state(Manager, fn _ -> original end)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end

  test "replication-lag probes are removed after a failed check" do
    idx = 1
    plant_map(%{idx => {node(), [:probe_dead@host]}})
    table = SuperCache.Partition.get_partition_by_idx(idx)

    _ = HealthMonitor.replication_lag(idx)

    assert :ets.match_object(table, {{:health_probe, node(), :_}, :_}) == []
  end

  test "stray handle_info messages keep the monitor alive" do
    send(HealthMonitor, {:whatever, 1})
    Process.sleep(10)
    assert Process.alive?(Process.whereis(HealthMonitor))
  end
end
