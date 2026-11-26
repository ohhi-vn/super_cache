defmodule SuperCache.Cluster.HealthMonitor do
  @moduledoc """
  Distributed cluster health monitor for SuperCache.

  Continuously monitors node connectivity, replication lag, partition health,
  and operation latency across the cluster. Provides real-time health status
  and anomaly detection.

  ## Health Checks

  The monitor performs these checks on a configurable interval:

  - **Node connectivity** — pings each live node via `:erpc` and measures RTT
  - **Replication lag** — writes a probe record and measures time until it
    appears on all replicas
  - **Partition balance** — checks that record counts are evenly distributed
  - **Error rate** — tracks the ratio of failed operations per node

  ## Health Status

  Each node reports a health status:

  | Status | Meaning |
  |--------|---------|
  | `:healthy` | All checks passing, latency within thresholds |
  | `:degraded` | Some checks failing or latency elevated |
  | `:unhealthy` | Node unreachable or critical checks failing |
  | `:unknown` | No health data available yet |

  ## Configuration

  | Option | Default | Description |
  |--------|---------|-------------|
  | `:interval_ms` | `5_000` | Health check interval in milliseconds |
  | `:latency_threshold_ms` | `100` | Max acceptable node RTT before `:degraded` |
  | `:replication_lag_threshold_ms` | `500` | Max acceptable replication lag |
  | `:error_rate_threshold` | `0.05` | Max acceptable error rate (5%) |
  | `:partition_imbalance_threshold` | `0.3` | Max acceptable partition size variance (30%) |
  | `:cycle_deadline_ms` | `30_000` | Maximum duration of one health-check cycle |

  ## Node-local checks and aggregation

  `run_health_checks/1` evaluates only `node()`; it never reads a peer's
  partition tables. `cluster_health/0` aggregates `local_node_health/0` from
  every live node through a bounded `Rpc.call/5` and falls back to the last
  cached aggregate for unreachable nodes, marking them `:degraded` or
  `:unknown` instead of omitting them.

  Replication-lag probes use `{:health_probe, node(), make_ref()}` and are
  removed in an `after` block even when the check fails or times out. Cycles
  are bounded by `:cycle_deadline_ms`; a timed-out cycle records connectivity
  as unhealthy and the remaining checks as unknown, and increments the
  `health/timeouts` counter. Checks are skipped until the cache has completed
  bootstrap (`Config.get_config(:started, false)` is true).

  ## Usage

      # Start with defaults
      SuperCache.Cluster.HealthMonitor.start_link([])

      # Get cluster health
      SuperCache.Cluster.HealthMonitor.cluster_health()
      # => %{status: :healthy, nodes: [...], checks: %{...}}

      # Get node-specific health
      SuperCache.Cluster.HealthMonitor.node_health(:"node1@127.0.0.1")
      # => %{status: :healthy, latency_ms: 12, checks: %{...}}

  ## Telemetry Events

  The monitor emits events via `:telemetry` (if available):

  - `[:super_cache, :health, :check]` — after each health check cycle
  - `[:super_cache, :health, :alert]` — when a health threshold is breached
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger
  require SuperCache.Log

  alias SuperCache.Cluster.{Manager, Metrics, Rpc}
  alias SuperCache.Cluster.Raft.{Group, Topology}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem
  alias SuperCache.{Config, Partition, Storage}

  # ── Configuration ─────────────────────────────────────────────────────────────

  @default_opts [
    interval_ms: 5_000,
    latency_threshold_ms: 100,
    replication_lag_threshold_ms: 500,
    error_rate_threshold: 0.05,
    partition_imbalance_threshold: 0.3
  ]

  # Health data stored in ETS for fast reads.
  # Key: {node, check_type} → value: %{status, timestamp, details}
  @health_table __MODULE__.HealthData

  # ── Public API ────────────────────────────────────────────────────────────────

  @doc """
  Starts the health monitor GenServer.

  ## Options

  See module documentation for configuration options.
  """
  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the overall cluster health status.

  ## Returns

      %{
        status: :healthy | :degraded | :unhealthy,
        timestamp: integer,
        nodes: [
          %{
            node: node(),
            status: :healthy | :degraded | :unhealthy | :unknown,
            latency_ms: non_neg_integer | nil,
            last_check: integer,
            checks: %{...}
          }
        ],
        summary: %{
          total_nodes: non_neg_integer,
          healthy: non_neg_integer,
          degraded: non_neg_integer,
          unhealthy: non_neg_integer
        }
      }
  """
  @spec cluster_health() :: map
  def cluster_health do
    nodes = live_nodes_safe()

    node_health =
      Enum.map(nodes, fn n ->
        if n == node() do
          local_node_health()
        else
          remote_node_health(n)
        end
      end)

    summary = %{
      total_nodes: length(nodes),
      healthy: Enum.count(node_health, &(&1.status == :healthy)),
      degraded: Enum.count(node_health, &(&1.status == :degraded)),
      unhealthy: Enum.count(node_health, &(&1.status == :unhealthy)),
      unknown: Enum.count(node_health, &(&1.status == :unknown))
    }

    overall_status =
      cond do
        summary.unhealthy > 0 -> :unhealthy
        summary.degraded > 0 -> :degraded
        true -> :healthy
      end

    %{
      status: overall_status,
      timestamp: System.monotonic_time(:millisecond),
      nodes: node_health,
      summary: summary
    }
  end

  @doc """
  Return the health map for the local node, computed from this node's own
  ETS tables and metrics. Called directly by `cluster_health/0` and remotely
  via `call/5` when aggregating peer health.
  """
  @spec local_node_health() :: map
  def local_node_health do
    checks =
      %{
        connectivity: get_check(node(), :connectivity),
        replication: get_check(node(), :replication),
        partitions: get_check(node(), :partitions),
        error_rate: get_check(node(), :error_rate)
      }

    health = build_node_health(node(), checks)

    topology = Topology.status()

    raft_status =
      if topology != nil and topology.status == :underprovisioned do
        if health.status == :unhealthy do
          health.status
        else
          :degraded
        end
      else
        health.status
      end

    health
    |> Map.put(:status, raft_status)
    |> Map.put(:raft, %{
      running: RaftSystem.running?(),
      groups: Group.tracked(),
      topology: topology
    })
  end

  @doc """
  Returns the health status for a specific node.

  For the local node this reads local ETS health data. For a remote node it
  asks that node for its own `local_node_health/0` through a bounded RPC and
  falls back to the last aggregated result (marked degraded) when the node
  is unreachable.
  """
  @spec node_health(node) :: map
  def node_health(node) do
    if node == node() do
      local_node_health()
    else
      remote_node_health(node)
    end
  end

  defp remote_node_health(target) do
    deadline = 5_000

    case Rpc.call(target, __MODULE__, :local_node_health, [], timeout: deadline) do
      {:ok, %{} = health} ->
        :ets.insert(@health_table, {{target, :aggregate}, health})
        health

      {:error, reason} ->
        if health_timeout?(reason.reason, reason.reason) do
          Metrics.increment(:health, :timeouts)
        end

        cached_or_degraded(target)
    end
  end

  defp cached_or_degraded(target) do
    case :ets.lookup(@health_table, {target, :aggregate}) do
      [{_, %{} = health}] ->
        %{health | status: :degraded, node: target}

      [] ->
        %{
          node: target,
          status: :unknown,
          latency_ms: nil,
          last_check: 0,
          checks: %{
            connectivity: %{status: :fail, timestamp: 0},
            replication: %{status: :unknown, timestamp: 0},
            partitions: %{status: :unknown, timestamp: 0},
            error_rate: %{status: :unknown, timestamp: 0}
          }
        }
    end
  end

  defp live_nodes_safe do
    Manager.live_nodes()
  catch
    :exit, _ -> [node()]
    _, _ -> [node()]
  end

  defp build_node_health(target, checks) do
    latency_ms =
      case checks.connectivity do
        %{latency_ms: ms} when is_integer(ms) -> ms
        _ -> nil
      end

    last_check =
      checks
      |> Map.values()
      |> Enum.map(&abs(Map.get(&1, :timestamp, 0)))
      |> Enum.max()

    %{
      node: target,
      status: derive_status(checks),
      latency_ms: latency_ms,
      last_check: last_check,
      checks: checks
    }
  end

  @doc """
  Returns the replication lag for a specific partition across all replicas.

  ## Returns

      %{
        partition_idx: non_neg_integer,
        primary: node(),
        replicas: [
          %{node: node(), lag_ms: non_neg_integer | nil, status: :synced | :lagging | :unknown}
        ]
      }
  """
  @spec replication_lag(non_neg_integer) :: map
  def replication_lag(partition_idx) do
    {primary, replicas} =
      try do
        Manager.get_replicas(partition_idx)
      catch
        :exit, _ -> {node(), []}
        _, _ -> {node(), []}
      end

    # A node cannot replicate to itself — filter out the current node so that
    # single-node clusters correctly report an empty replicas list.
    other_replicas = Enum.reject(replicas, &(&1 == node()))

    replica_status =
      Enum.map(other_replicas, fn replica ->
        lag = measure_replication_lag(partition_idx, primary, replica)

        threshold =
          try do
            Config.get_config(:replication_lag_threshold_ms, 500)
          catch
            :exit, _ -> 500
            _, _ -> 500
          end

        %{
          node: replica,
          lag_ms: lag,
          status:
            cond do
              is_nil(lag) -> :unknown
              lag <= threshold -> :synced
              true -> :lagging
            end
        }
      end)

    %{
      partition_idx: partition_idx,
      primary: primary,
      replicas: replica_status
    }
  end

  @doc """
  Returns partition balance statistics across all nodes.

  ## Returns

      %{
        total_records: non_neg_integer,
        partition_count: non_neg_integer,
        avg_records_per_partition: float,
        max_imbalance: float,
        partitions: [
          %{idx: non_neg_integer, record_count: non_neg_integer, primary: node()}
        ]
      }
  """
  @spec partition_balance() :: map
  def partition_balance do
    case safe_num_partitions() do
      0 -> empty_partition_balance()
      num_partitions -> build_partition_balance(num_partitions)
    end
  end

  defp empty_partition_balance do
    %{
      total_records: 0,
      partitions: [],
      partition_count: 0,
      avg_records_per_partition: 0.0,
      max_imbalance: 0.0
    }
  end

  defp build_partition_balance(num_partitions) do
    partitions = Enum.map(0..(num_partitions - 1), &partition_stat/1)
    total = Enum.reduce(partitions, 0, &(&1.record_count + &2))
    avg = total / num_partitions

    %{
      total_records: total,
      partition_count: num_partitions,
      avg_records_per_partition: avg,
      max_imbalance: max_imbalance(partitions, avg),
      partitions: partitions
    }
  end

  defp partition_stat(idx) do
    {primary, _replicas} = safe_replicas(idx)
    %{idx: idx, record_count: safe_partition_count(idx), primary: primary}
  end

  defp safe_num_partitions do
    Partition.get_num_partition()
  catch
    :exit, _ -> 0
    _, _ -> 0
  end

  defp safe_replicas(idx) do
    Manager.get_replicas(idx)
  catch
    :exit, _ -> {node(), []}
    _, _ -> {node(), []}
  end

  defp safe_partition_count(idx) do
    idx |> safe_partition_table() |> count_for_table()
  end

  defp safe_partition_table(idx) do
    Partition.get_partition_by_idx(idx)
  catch
    _, _ -> nil
  end

  defp count_for_table(nil), do: 0

  defp count_for_table(table) do
    {_, count} = Storage.stats(table)
    if is_integer(count), do: count, else: 0
  catch
    _, _ -> 0
  end

  defp max_imbalance(_partitions, avg) when avg == 0, do: 0.0

  defp max_imbalance(partitions, avg) do
    partitions
    |> Enum.map(fn p -> abs(p.record_count - avg) / avg end)
    |> Enum.max()
  end

  @doc """
  Forces an immediate health check cycle.

  Useful for testing or when an external monitoring system needs fresh data.
  """
  @spec force_check() :: :ok
  def force_check do
    GenServer.call(__MODULE__, :force_check, 30_000)
  end

  # ── GenServer Callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    merged_opts = Keyword.merge(@default_opts, opts)

    # Create health data ETS table
    _ =
      :ets.new(@health_table, [
        :set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    Logger.info(
      "super_cache, health_monitor, started with interval=#{merged_opts[:interval_ms]}ms"
    )

    # Schedule first check
    schedule_check(merged_opts[:interval_ms])

    {:ok, %{opts: merged_opts}}
  end

  @impl true
  def handle_call(:force_check, _from, %{opts: opts} = state) do
    run_health_checks(opts)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:health_check, %{opts: opts} = state) do
    run_health_checks(opts)
    schedule_check(opts[:interval_ms])
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Private — Health Check Implementation ────────────────────────────────────

  defp schedule_check(interval_ms) do
    Process.send_after(self(), :health_check, interval_ms)
  end

  defp run_health_checks(opts) do
    t0 = System.monotonic_time(:millisecond)

    if Config.get_config(:started, false) != true do
      SuperCache.Log.debug(fn ->
        "super_cache, health_monitor, skipping health checks (cache not started)"
      end)
    else
      # Health checks evaluate only the local node. Remote nodes are checked
      # when cluster_health/0 aggregates their own local_node_health/0.
      deadline = Keyword.get(opts, :cycle_deadline_ms, 30_000)

      task =
        Task.async(fn ->
          try do
            check_connectivity(node(), opts)
            check_replication(node(), opts)
            check_partitions(node(), opts)
            check_error_rate(node(), opts)
          catch
            _, _ -> :ok
          end
        end)

      case Task.yield(task, deadline) do
        nil ->
          Task.shutdown(task, :brutal_kill)
          Metrics.increment(:health, :timeouts)
          record_cycle_timeout()

        _result ->
          :ok
      end
    end

    elapsed = System.monotonic_time(:millisecond) - t0

    SuperCache.Log.debug(fn ->
      "super_cache, health_monitor, check cycle completed in #{elapsed}ms"
    end)

    emit_telemetry(:check, %{duration_ms: elapsed, node_count: 1})
  end

  # If a cycle is killed at the deadline we cannot trust any partial check
  # results; record connectivity as unhealthy and the rest as unknown.
  defp record_cycle_timeout do
    now = System.monotonic_time(:millisecond)
    store_check(node(), :connectivity, %{status: :fail, latency_ms: nil, timestamp: now})
    store_check(node(), :replication, %{status: :unknown, timestamp: now})
    store_check(node(), :partitions, %{status: :unknown, timestamp: now})
    store_check(node(), :error_rate, %{status: :unknown, timestamp: now})
  end

  # ── Private — Individual Checks ──────────────────────────────────────────────

  defp check_connectivity(node, opts) do
    threshold = opts[:latency_threshold_ms]

    result =
      try do
        t0 = System.monotonic_time(:millisecond)
        :erpc.call(node, :erlang, :node, [], 5_000)
        latency = System.monotonic_time(:millisecond) - t0

        status = if latency <= threshold, do: :pass, else: :degraded

        %{
          status: status,
          latency_ms: latency,
          timestamp: System.monotonic_time(:millisecond)
        }
      catch
        kind, reason ->
          if health_timeout?(kind, reason) do
            Metrics.increment(:health, :timeouts)
          end

          Logger.warning(
            "super_cache, health_monitor, connectivity check failed for #{inspect(node)}: #{inspect({kind, reason})}"
          )

          %{
            status: :fail,
            latency_ms: nil,
            timestamp: System.monotonic_time(:millisecond),
            error: {kind, reason}
          }
      end

    store_check(node, :connectivity, result)

    if result.status == :fail do
      emit_telemetry(:alert, %{
        type: :connectivity_failure,
        node: node,
        error: result[:error]
      })
    end
  end

  defp check_replication(node, opts) do
    threshold = opts[:replication_lag_threshold_ms]
    primary_partitions = primary_partition_indices(node)

    if primary_partitions == [] do
      store_check(node, :replication, %{
        status: :unknown,
        lag_ms: nil,
        timestamp: System.monotonic_time(:millisecond),
        reason: :not_primary
      })

      :ok
    else
      check_replication_lag(node, hd(primary_partitions), threshold)
    end
  end

  defp check_replication_lag(node, sample_idx, threshold) do
    {_, all_replicas} = safe_replicas(sample_idx)
    # A node cannot replicate to itself — filter out the current node.
    replicas = Enum.reject(all_replicas, &(&1 == node()))

    if replicas == [] do
      store_check(node, :replication, %{
        status: :pass,
        lag_ms: 0,
        timestamp: System.monotonic_time(:millisecond),
        reason: :no_replicas
      })
    else
      lag = measure_replication_lag(sample_idx, node, hd(replicas))
      status = replication_status(lag, threshold)

      store_check(node, :replication, %{
        status: status,
        lag_ms: lag,
        timestamp: System.monotonic_time(:millisecond)
      })

      maybe_alert_replication(node, status, lag, threshold)
    end
  end

  defp replication_status(lag, _threshold) when is_nil(lag), do: :unknown
  defp replication_status(lag, threshold) when lag <= threshold, do: :pass
  defp replication_status(_lag, _threshold), do: :degraded

  defp maybe_alert_replication(_node, status, _lag, _threshold) when status != :degraded, do: :ok

  defp maybe_alert_replication(node, :degraded, lag, threshold) do
    emit_telemetry(:alert, %{
      type: :replication_lag,
      node: node,
      lag_ms: lag,
      threshold_ms: threshold
    })
  end

  defp check_partitions(node, opts) do
    threshold = opts[:partition_imbalance_threshold]
    primary_partitions = primary_partition_indices(node)

    if primary_partitions == [] do
      store_check(node, :partitions, %{
        status: :unknown,
        imbalance: nil,
        timestamp: System.monotonic_time(:millisecond),
        reason: :not_primary
      })
    else
      store_partition_balance(node, primary_partitions, threshold)
    end
  end

  defp store_partition_balance(node, primary_partitions, threshold) do
    imbalance = partition_imbalance(primary_partitions)
    status = if imbalance <= threshold, do: :pass, else: :degraded

    store_check(node, :partitions, %{
      status: status,
      imbalance: imbalance,
      timestamp: System.monotonic_time(:millisecond)
    })

    maybe_alert_imbalance(node, status, imbalance, threshold)
  end

  defp partition_imbalance(primary_partitions) do
    sizes =
      Enum.map(primary_partitions, fn idx ->
        table = Partition.get_partition_by_idx(idx)
        {_, count} = Storage.stats(table)
        count
      end)

    avg = Enum.sum(sizes) / length(sizes)

    if avg > 0 do
      sizes |> Enum.map(fn s -> abs(s - avg) / avg end) |> Enum.max()
    else
      0.0
    end
  end

  defp maybe_alert_imbalance(_node, status, _imbalance, _threshold) when status != :degraded,
    do: :ok

  defp maybe_alert_imbalance(node, :degraded, imbalance, threshold) do
    emit_telemetry(:alert, %{
      type: :partition_imbalance,
      node: node,
      imbalance: imbalance,
      threshold: threshold
    })
  end

  defp primary_partition_indices(node) do
    0..(Partition.get_num_partition() - 1)
    |> Enum.filter(fn idx ->
      {primary, _} = Manager.get_replicas(idx)
      primary == node
    end)
  catch
    :exit, _ -> []
    _, _ -> []
  end

  defp check_error_rate(node, opts) do
    threshold = opts[:error_rate_threshold]

    # Get error rate from metrics
    api_metrics =
      try do
        :erpc.call(node, Metrics, :get_all, [{:api, :put}], 5_000)
      catch
        _, _ -> %{}
      end

    calls = Map.get(api_metrics, :calls, 0)
    errors = Map.get(api_metrics, :errors, 0)

    rate = if calls > 0, do: errors / calls, else: 0.0

    status = if rate <= threshold, do: :pass, else: :degraded

    store_check(node, :error_rate, %{
      status: status,
      rate: rate,
      calls: calls,
      errors: errors,
      timestamp: System.monotonic_time(:millisecond)
    })

    if status == :degraded do
      emit_telemetry(:alert, %{
        type: :high_error_rate,
        node: node,
        rate: rate,
        threshold: threshold
      })
    end
  end

  # ── Private — Replication Lag Measurement ─────────────────────────────────────

  defp measure_replication_lag(partition_idx, _primary, replica) do
    probe_key = {:health_probe, node(), make_ref()}
    probe_value = System.monotonic_time(:microsecond)
    table = Partition.get_partition_by_idx(partition_idx)

    try do
      # Write probe to primary
      Storage.put({probe_key, probe_value}, table)

      # Poll replica until it sees the probe
      t0 = System.monotonic_time(:millisecond)
      max_wait = 2_000

      poll_replica(replica, partition_idx, probe_key, probe_value, t0, max_wait)
    catch
      _, _ -> nil
    after
      # Always remove the probe from the primary and every replica, even when
      # the poll timed out or the replica is unreachable.
      cleanup_probe(probe_key, table, replica)
    end
  end

  defp cleanup_probe(probe_key, table, replica) do
    try do
      Storage.delete(probe_key, table)
    catch
      _, _ -> :ok
    end

    try do
      :erpc.call(replica, Storage, :delete, [probe_key, table], 1_000)
    catch
      _, _ -> :ok
    end
  end

  defp poll_replica(replica, partition_idx, probe_key, probe_value, t0, max_wait) do
    if System.monotonic_time(:millisecond) - t0 > max_wait do
      Metrics.increment(:health, :timeouts)
      nil
    else
      result =
        try do
          table = Partition.get_partition_by_idx(partition_idx)
          :erpc.call(replica, Storage, :get, [probe_key, table], 1_000)
        catch
          _, _ -> []
        end

      case result do
        [{^probe_key, ^probe_value}] ->
          System.monotonic_time(:millisecond) - t0

        _ ->
          Process.sleep(10)
          poll_replica(replica, partition_idx, probe_key, probe_value, t0, max_wait)
      end
    end
  end

  # ── Private — Health Data Storage ─────────────────────────────────────────────

  defp store_check(node, check_type, data) do
    :ets.insert(@health_table, {{node, check_type}, data})
  end

  defp health_timeout?(:error, {:erpc, :timeout}), do: true
  defp health_timeout?(_, :timeout), do: true
  defp health_timeout?(:exit, {:timeout, _}), do: true
  defp health_timeout?(_, _), do: false

  defp get_check(node, check_type) do
    case :ets.lookup(@health_table, {node, check_type}) do
      [{_, data}] -> data
      [] -> %{status: :unknown, timestamp: 0}
    end
  end

  defp derive_status(checks) do
    statuses = Enum.map(checks, fn {_type, check} -> check.status end)

    cond do
      :fail in statuses -> :unhealthy
      :degraded in statuses -> :degraded
      # Only propagate :unknown if connectivity itself is unknown.
      # In single-node clusters, replication/partitions checks are :unknown
      # (not primary, no replicas) but connectivity passes — the node is healthy.
      checks.connectivity.status == :unknown -> :unknown
      true -> :healthy
    end
  end

  # ── Private — Telemetry ──────────────────────────────────────────────────────

  @dialyzer {:nowarn_function, emit_telemetry: 2}
  defp emit_telemetry(event, metadata) do
    # `:telemetry` is an optional dependency — emit only when the module and
    # function are actually loaded. Handler exceptions still propagate to
    # telemetry's own handler supervision rather than being swallowed here.
    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      Function.capture(:telemetry, :execute, 3).([:super_cache, :health, event], %{}, metadata)
    else
      :ok
    end
  end
end
