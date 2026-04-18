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

  alias SuperCache.Cluster.{Manager, Metrics}
  alias SuperCache.{Partition, Storage, Config}

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

  # Probe key used for replication lag measurement.
  @probe_key {:health_probe, :latency}

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
  def cluster_health() do
    nodes =
      try do
        Manager.live_nodes()
      catch
        :exit, _ -> [node()]
        _, _ -> [node()]
      end

    node_health =
      Enum.map(nodes, fn n ->
        node_health(n)
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
  Returns the health status for a specific node.

  ## Returns

      %{
        node: node(),
        status: :healthy | :degraded | :unhealthy | :unknown,
        latency_ms: non_neg_integer | nil,
        last_check: integer,
        checks: %{
          connectivity: %{status: :pass | :fail, latency_ms: non_neg_integer | nil},
          replication: %{status: :pass | :fail | :unknown, lag_ms: non_neg_integer | nil},
          partitions: %{status: :pass | :fail | :unknown, imbalance: float | nil},
          error_rate: %{status: :pass | :fail | :unknown, rate: float | nil}
        }
      }
  """
  @spec node_health(node) :: map
  def node_health(node) do
    # Verify cluster infrastructure is available
    _nodes =
      try do
        Manager.live_nodes()
      catch
        :exit, _ -> [node()]
        _, _ -> [node()]
      end

    checks =
      %{
        connectivity: get_check(node, :connectivity),
        replication: get_check(node, :replication),
        partitions: get_check(node, :partitions),
        error_rate: get_check(node, :error_rate)
      }

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
      node: node,
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
  def partition_balance() do
    num_partitions =
      try do
        Partition.get_num_partition()
      catch
        :exit, _ -> 0
        _, _ -> 0
      end

    if num_partitions == 0 do
      %{total_records: 0, partitions: [], partition_count: 0, avg_records_per_partition: 0.0, max_imbalance: 0.0}
    else
      partitions =
      Enum.map(0..(num_partitions - 1), fn idx ->
        {primary, _} =
          try do
            Manager.get_replicas(idx)
          catch
            :exit, _ -> {node(), []}
            _, _ -> {node(), []}
          end

        table =
          try do
            Partition.get_partition_by_idx(idx)
          catch
            _, _ -> nil
          end

        count =
          if table do
            try do
              {_, c} = Storage.stats(table)
              if is_integer(c), do: c, else: 0
            catch
              _, _ -> 0
            end
          else
            0
          end

        %{
          idx: idx,
          record_count: count,
          primary: primary
        }
      end)

    total = Enum.reduce(partitions, 0, &(&1.record_count + &2))
    avg = if num_partitions > 0, do: total / num_partitions, else: 0

    max_imbalance =
      if avg > 0 do
        partitions
        |> Enum.map(fn p -> abs(p.record_count - avg) / avg end)
        |> Enum.max()
      else
        0.0
      end

    %{
      total_records: total,
      partition_count: num_partitions,
      avg_records_per_partition: avg,
      max_imbalance: max_imbalance,
      partitions: partitions
    }
  end
  end

  @doc """
  Forces an immediate health check cycle.

  Useful for testing or when an external monitoring system needs fresh data.
  """
  @spec force_check() :: :ok
  def force_check() do
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

    # Gracefully handle missing cluster infrastructure (e.g., during tests or before bootstrap)
    # Use catch to handle :exit from dead GenServer, not just rescue for exceptions
    nodes =
      try do
        Manager.live_nodes()
      catch
        :exit, _ -> []
        _, _ -> []
      end

    if nodes == [] do
      Logger.debug("super_cache, health_monitor, skipping health checks (cluster not ready)")
    else
      # Run checks in parallel across all nodes, catching any crashes
      # so one failing check doesn't crash the whole cycle
      tasks =
        Enum.map(nodes, fn node ->
          Task.async(fn ->
            try do
              check_connectivity(node, opts)
              check_replication(node, opts)
              check_partitions(node, opts)
              check_error_rate(node, opts)
            catch
              _, _ -> :ok
            end
          end)
        end)

      # Use yield_many with timeout to avoid blocking on slow/dead nodes
      Task.yield_many(tasks, 30_000)
      |> Enum.each(fn {task, _result} ->
        Task.shutdown(task, :brutal_kill)
      end)
    end

    elapsed = System.monotonic_time(:millisecond) - t0

    SuperCache.Log.debug(fn ->
      "super_cache, health_monitor, check cycle completed in #{elapsed}ms for #{length(nodes)} node(s)"
    end)

    # Emit telemetry event if available
    emit_telemetry(:check, %{duration_ms: elapsed, node_count: length(nodes)})
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

    # Only check replication on primary nodes
    primary_partitions =
      try do
        0..(Partition.get_num_partition() - 1)
        |> Enum.filter(fn idx ->
          {primary, _} = Manager.get_replicas(idx)
          primary == node
        end)
      catch
        :exit, _ -> []
        _, _ -> []
      end

    if primary_partitions == [] do
      store_check(node, :replication, %{
        status: :unknown,
        lag_ms: nil,
        timestamp: System.monotonic_time(:millisecond),
        reason: :not_primary
      })

      :ok
    else
      # Measure replication lag on a sample partition
      sample_idx = hd(primary_partitions)
      {_, all_replicas} =
        try do
          Manager.get_replicas(sample_idx)
        catch
          :exit, _ -> {node(), []}
          _, _ -> {node(), []}
        end

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
        # Write a probe and measure time until replicas see it
        lag = measure_replication_lag(sample_idx, node, hd(replicas))

        status =
          cond do
            is_nil(lag) -> :unknown
            lag <= threshold -> :pass
            true -> :degraded
          end

        store_check(node, :replication, %{
          status: status,
          lag_ms: lag,
          timestamp: System.monotonic_time(:millisecond)
        })

        if status == :degraded do
          emit_telemetry(:alert, %{
            type: :replication_lag,
            node: node,
            lag_ms: lag,
            threshold_ms: threshold
          })
        end
      end
    end
  end

  defp check_partitions(node, opts) do
    threshold = opts[:partition_imbalance_threshold]

    # Get partition sizes for this node's primary partitions
    primary_partitions =
      try do
        0..(Partition.get_num_partition() - 1)
        |> Enum.filter(fn idx ->
          {primary, _} = Manager.get_replicas(idx)
          primary == node
        end)
      catch
        :exit, _ -> []
        _, _ -> []
      end

    if primary_partitions == [] do
      store_check(node, :partitions, %{
        status: :unknown,
        imbalance: nil,
        timestamp: System.monotonic_time(:millisecond),
        reason: :not_primary
      })
    else
      sizes =
        Enum.map(primary_partitions, fn idx ->
          table = Partition.get_partition_by_idx(idx)
          {_, count} = Storage.stats(table)
          count
        end)

      avg = Enum.sum(sizes) / length(sizes)

      imbalance =
        if avg > 0 do
          sizes
          |> Enum.map(fn s -> abs(s - avg) / avg end)
          |> Enum.max()
        else
          0.0
        end

      status = if imbalance <= threshold, do: :pass, else: :degraded

      store_check(node, :partitions, %{
        status: status,
        imbalance: imbalance,
        timestamp: System.monotonic_time(:millisecond)
      })

      if status == :degraded do
        emit_telemetry(:alert, %{
          type: :partition_imbalance,
          node: node,
          imbalance: imbalance,
          threshold: threshold
        })
      end
    end
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
    probe_value = System.monotonic_time(:microsecond)

    try do
      # Write probe to primary
      table = Partition.get_partition_by_idx(partition_idx)
      Storage.put({@probe_key, probe_value}, table)

      # Poll replica until it sees the probe
      t0 = System.monotonic_time(:millisecond)
      max_wait = 2_000

      lag = poll_replica(replica, partition_idx, @probe_key, probe_value, t0, max_wait)

      # Clean up probe
      Storage.delete(@probe_key, table)

      lag
    catch
      _, _ -> nil
    end
  end

  defp poll_replica(replica, partition_idx, probe_key, probe_value, t0, max_wait) do
    if System.monotonic_time(:millisecond) - t0 > max_wait do
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

  defp emit_telemetry(event, metadata) do
    # Try to emit telemetry if available, silently ignore if not
    try do
      :telemetry.execute([:super_cache, :health, event], %{}, metadata)
    catch
      _, _ -> :ok
    end
  end
end
