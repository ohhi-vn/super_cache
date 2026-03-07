defmodule SuperCache.Cluster.Stats do
  @moduledoc """
  Observability utilities for a running SuperCache cluster.

  ## Available reports

  | Function                  | What it shows                                        |
  |---------------------------|------------------------------------------------------|
  | `cluster/0`               | Full cluster overview — nodes, partitions, ETS sizes |
  | `partitions/0`            | Per-partition primary/replica assignment + ETS count |
  | `primary_partitions/0`    | Partitions owned (primary) by this node              |
  | `replica_partitions/0`    | Partitions this node replicates but does not own     |
  | `node_partitions/1`       | Partition ownership for any specific node            |
  | `three_phase_commit/0`    | Counters for the 3PC coordinator (this node)         |
  | `api/0`                   | Per-operation call counters + latency percentiles    |
  | `print/1`                 | Pretty-print any stats map to the console            |

  ## Quick start

      alias SuperCache.Cluster.Stats

      Stats.cluster()        |> Stats.print()
      Stats.partitions()     |> Stats.print()
      Stats.three_phase_commit() |> Stats.print()
      Stats.api()            |> Stats.print()

  ## Telemetry integration

  All counters are stored in a dedicated ETS table owned by
  `SuperCache.Cluster.Metrics` and are updated via
  `SuperCache.Cluster.Stats.record/2`.  You can attach your own
  `:telemetry` handler or periodically call `Stats.api/0` to ship
  metrics to an external system.
  """

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, TxnRegistry, Metrics}

  # ── Cluster overview ─────────────────────────────────────────────────────────

  @doc """
  Return a full cluster overview map.

  Fields:

  - `:nodes` — list of live nodes
  - `:node_count` — number of live nodes
  - `:replication_factor` — configured replication factor
  - `:replication_mode` — `:async | :sync | :strong`
  - `:num_partitions` — total partition count
  - `:partitions` — list of per-partition maps (see `partitions/0`)
  - `:total_records` — sum of ETS record counts across all local partitions

  ## Example

      iex> SuperCache.Cluster.Stats.cluster()
      %{
        nodes: [:"primary@127.0.0.1", :"replica@127.0.0.1"],
        node_count: 2,
        replication_factor: 2,
        replication_mode: :async,
        num_partitions: 8,
        total_records: 1_042,
        partitions: [...]
      }
  """
  @spec cluster() :: map
  def cluster() do
    parts = partitions()
    total = parts |> Enum.map(& &1.local_record_count) |> Enum.sum()

    %{
      nodes:              Manager.live_nodes(),
      node_count:         Manager.live_nodes() |> length(),
      replication_factor: Config.get_config(:replication_factor, 1),
      replication_mode:   Config.get_config(:replication_mode, :async),
      num_partitions:     Partition.get_num_partition(),
      total_records:      total,
      partitions:         parts
    }
  end

  # ── Partition maps ───────────────────────────────────────────────────────────

  @doc """
  Return one map per partition describing ownership and local ETS size.

  Each map contains:

  - `:idx` — partition index
  - `:table` — ETS table atom
  - `:primary` — node that owns this partition
  - `:replicas` — list of replica nodes
  - `:role` — `:primary | :replica | :none` (this node's role)
  - `:local_record_count` — number of records in the local ETS table

  ## Example

      iex> SuperCache.Cluster.Stats.partitions()
      [
        %{idx: 0, table: :"SuperCache.Storage.Ets_0", primary: :"a@host",
          replicas: [:"b@host"], role: :primary, local_record_count: 128},
        ...
      ]
  """
  @spec partitions() :: [map]
  def partitions() do
    me = node()

    Enum.map(0..(Partition.get_num_partition() - 1), fn idx ->
      {primary, replicas} = Manager.get_replicas(idx)
      table     = Partition.get_partition_by_idx(idx)
      {_, count} = Storage.stats(table)

      role =
        cond do
          primary == me    -> :primary
          me in replicas   -> :replica
          true             -> :none
        end

      %{
        idx:                 idx,
        table:               table,
        primary:             primary,
        replicas:            replicas,
        role:                role,
        local_record_count:  count
      }
    end)
  end

  @doc """
  Return only the partitions for which this node is the **primary**.

  ## Example

      iex> SuperCache.Cluster.Stats.primary_partitions()
      [
        %{idx: 0, primary: :"a@host", replicas: [:"b@host"],
          role: :primary, local_record_count: 128},
        ...
      ]
  """
  @spec primary_partitions() :: [map]
  def primary_partitions() do
    partitions() |> Enum.filter(&(&1.role == :primary))
  end

  @doc """
  Return only the partitions for which this node is a **replica**.

  ## Example

      iex> SuperCache.Cluster.Stats.replica_partitions()
      [
        %{idx: 3, primary: :"b@host", replicas: [:"a@host"],
          role: :replica, local_record_count: 95},
        ...
      ]
  """
  @spec replica_partitions() :: [map]
  def replica_partitions() do
    partitions() |> Enum.filter(&(&1.role == :replica))
  end

  @doc """
  Return the partition ownership summary for `target_node`.

  Returns a map with:

  - `:node` — the queried node
  - `:primary_count` — number of partitions it owns as primary
  - `:replica_count` — number of partitions it replicates
  - `:primary_partitions` — list of partition indices where it is primary
  - `:replica_partitions` — list of partition indices where it is replica

  ## Example

      iex> SuperCache.Cluster.Stats.node_partitions(:"replica@127.0.0.1")
      %{
        node: :"replica@127.0.0.1",
        primary_count: 4,
        replica_count: 4,
        primary_partitions: [1, 3, 5, 7],
        replica_partitions: [0, 2, 4, 6]
      }
  """
  @spec node_partitions(node) :: map
  def node_partitions(target_node) do
    all = partitions()

    primary_idxs =
      all
      |> Enum.filter(&(&1.primary == target_node))
      |> Enum.map(& &1.idx)

    replica_idxs =
      all
      |> Enum.filter(&(target_node in &1.replicas))
      |> Enum.map(& &1.idx)

    %{
      node:                target_node,
      primary_count:       length(primary_idxs),
      replica_count:       length(replica_idxs),
      primary_partitions:  primary_idxs,
      replica_partitions:  replica_idxs
    }
  end

  # ── Three-phase commit metrics ───────────────────────────────────────────────

  @doc """
  Return three-phase commit counters for this node.

  Counters:

  - `:in_flight` — number of transactions currently in the TxnRegistry
  - `:committed` — total successfully committed transactions
  - `:aborted` — total aborted transactions (any phase)
  - `:prepare_failures` — transactions that failed at PREPARE phase
  - `:pre_commit_failures` — transactions that failed at PRE_COMMIT phase
  - `:commit_failures` — transactions that failed at COMMIT phase
  - `:recovered_committed` — in-doubt :pre_committed entries resolved on startup
  - `:recovered_aborted` — in-doubt :prepared entries rolled back on startup
  - `:avg_commit_latency_us` — rolling average commit latency in microseconds
  - `:p99_commit_latency_us` — 99th-percentile commit latency (sampled window)

  ## Example

      iex> SuperCache.Cluster.Stats.three_phase_commit()
      %{
        in_flight: 0,
        committed: 1_200,
        aborted: 3,
        prepare_failures: 1,
        pre_commit_failures: 1,
        commit_failures: 1,
        recovered_committed: 0,
        recovered_aborted: 0,
        avg_commit_latency_us: 1_840,
        p99_commit_latency_us: 4_210
      }
  """
  @spec three_phase_commit() :: map
  def three_phase_commit() do
    c = Metrics.get_all(:tpc)

    {avg, p99} = latency_stats(:tpc_commit_latency_us)

    %{
      in_flight:              TxnRegistry.count(),
      committed:              Map.get(c, :committed, 0),
      aborted:                Map.get(c, :aborted, 0),
      prepare_failures:       Map.get(c, :prepare_failures, 0),
      pre_commit_failures:    Map.get(c, :pre_commit_failures, 0),
      commit_failures:        Map.get(c, :commit_failures, 0),
      recovered_committed:    Map.get(c, :recovered_committed, 0),
      recovered_aborted:      Map.get(c, :recovered_aborted, 0),
      avg_commit_latency_us:  avg,
      p99_commit_latency_us:  p99
    }
  end

  # ── API call metrics ─────────────────────────────────────────────────────────

  @doc """
  Return per-operation call counters and latency statistics for this node.

  Each entry in the returned map is keyed by operation name and contains:

  - `:calls` — total number of calls
  - `:errors` — calls that returned `{:error, _}`
  - `:avg_us` — rolling average latency in microseconds
  - `:p99_us` — 99th-percentile latency (sampled window)

  Operations tracked: `:put`, `:get_local`, `:get_primary`, `:get_quorum`,
  `:delete`, `:delete_all`, `:delete_match`, `:replicate_async`,
  `:replicate_sync`, `:replicate_strong`.

  ## Example

      iex> SuperCache.Cluster.Stats.api()
      %{
        put:              %{calls: 5_000, errors: 2, avg_us: 210, p99_us: 890},
        get_local:        %{calls: 18_000, errors: 0, avg_us: 12,  p99_us: 45},
        get_primary:      %{calls: 300,   errors: 0, avg_us: 540, p99_us: 1_200},
        get_quorum:       %{calls: 50,    errors: 0, avg_us: 620, p99_us: 1_400},
        delete:           %{calls: 800,   errors: 0, avg_us: 190, p99_us: 750},
        delete_all:       %{calls: 3,     errors: 0, avg_us: 4_200, p99_us: 6_000},
        delete_match:     %{calls: 120,   errors: 0, avg_us: 310, p99_us: 900},
        replicate_async:  %{calls: 5_000, errors: 5, avg_us: 95,  p99_us: 400},
        replicate_sync:   %{calls: 0,     errors: 0, avg_us: 0,   p99_us: 0},
        replicate_strong: %{calls: 0,     errors: 0, avg_us: 0,   p99_us: 0}
      }
  """
  @spec api() :: map
  def api() do
    ops = [
      :put, :get_local, :get_primary, :get_quorum,
      :delete, :delete_all, :delete_match,
      :replicate_async, :replicate_sync, :replicate_strong
    ]

    Map.new(ops, fn op ->
      c = Metrics.get_all({:api, op})
      {avg, p99} = latency_stats({:api_latency_us, op})

      {op, %{
        calls:   Map.get(c, :calls, 0),
        errors:  Map.get(c, :errors, 0),
        avg_us:  avg,
        p99_us:  p99
      }}
    end)
  end

  # ── Pretty printer ───────────────────────────────────────────────────────────

  @doc """
  Pretty-print any stats map or list returned by this module to stdout.

  Handles the nested structures returned by `cluster/0`, `partitions/0`,
  `three_phase_commit/0`, and `api/0`.

  ## Example

      SuperCache.Cluster.Stats.cluster() |> SuperCache.Cluster.Stats.print()

      # ╔══════════════════════════════════════╗
      # ║       SuperCache Cluster Stats       ║
      # ╚══════════════════════════════════════╝
      # nodes            : [a@host, b@host]
      # node_count       : 2
      # replication_mode : async
      # ...
  """
  @spec print(map | [map]) :: :ok
  def print(stats) when is_map(stats) do
    IO.puts(banner("SuperCache Stats"))
    print_map(stats, 0)
    IO.puts("")
    :ok
  end

  def print(stats) when is_list(stats) do
    IO.puts(banner("SuperCache Stats"))
    stats
    |> Enum.with_index(1)
    |> Enum.each(fn {item, i} ->
      IO.puts("  [#{i}]")
      print_map(item, 4)
    end)
    IO.puts("")
    :ok
  end

  # ── Instrumentation helpers (called by Router, Replicator, 3PC) ──────────────

  @doc """
  Record a completed API call.

  Called by `SuperCache.Cluster.Router` and `SuperCache.Cluster.Replicator`
  after each operation.

      Stats.record({:api, :put}, %{latency_us: 210, error: false})
  """
  @spec record(term, map) :: :ok
  def record(key, %{latency_us: lat, error: err}) do
    Metrics.increment({:api, key}, :calls)
    if err, do: Metrics.increment({:api, key}, :errors)
    Metrics.push_latency({:api_latency_us, key}, lat)
    :ok
  end

  @doc """
  Record a completed 3PC transaction.

      Stats.record_tpc(:committed, latency_us: 1_840)
      Stats.record_tpc(:aborted, phase: :prepare)
  """
  @spec record_tpc(atom, keyword) :: :ok
  def record_tpc(:committed, opts) do
    Metrics.increment(:tpc, :committed)
    if lat = Keyword.get(opts, :latency_us) do
      Metrics.push_latency(:tpc_commit_latency_us, lat)
    end
    :ok
  end

  def record_tpc(:aborted, opts) do
    Metrics.increment(:tpc, :aborted)
    case Keyword.get(opts, :phase) do
      :prepare    -> Metrics.increment(:tpc, :prepare_failures)
      :pre_commit -> Metrics.increment(:tpc, :pre_commit_failures)
      :commit     -> Metrics.increment(:tpc, :commit_failures)
      _           -> :ok
    end
    :ok
  end

  def record_tpc(:recovered_committed, _opts), do: Metrics.increment(:tpc, :recovered_committed)
  def record_tpc(:recovered_aborted,   _opts), do: Metrics.increment(:tpc, :recovered_aborted)
  def record_tpc(_event, _opts),               do: :ok

  # ── Private ──────────────────────────────────────────────────────────────────

  # Compute {avg, p99} from the latency sample ring buffer.
  defp latency_stats(key) do
    case Metrics.get_latency_samples(key) do
      [] ->
        {0, 0}

      samples ->
        sorted = Enum.sort(samples)
        n      = length(sorted)
        avg    = div(Enum.sum(sorted), n)
        p99    = Enum.at(sorted, trunc(n * 0.99))
        {avg, p99}
    end
  end

  defp banner(title) do
    pad = 38
    top = "╔" <> String.duplicate("═", pad) <> "╗"
    centered = title
               |> String.pad_leading(div(pad + String.length(title), 2))
               |> String.pad_trailing(pad)
    mid = "║" <> centered <> "║"
    bot = "╚" <> String.duplicate("═", pad) <> "╝"
    Enum.join([top, mid, bot], "\n")
  end

  defp print_map(map, indent) do
    pad = String.duplicate(" ", indent)

    Enum.each(map, fn {k, v} ->
      key_str = k |> to_string() |> String.pad_trailing(24)

      case v do
        v when is_map(v) ->
          IO.puts("#{pad}#{key_str}:")
          print_map(v, indent + 4)

        v when is_list(v) and length(v) > 0 and is_map(hd(v)) ->
          IO.puts("#{pad}#{key_str}:")
          v |> Enum.with_index(1) |> Enum.each(fn {item, i} ->
            IO.puts("#{pad}  [#{i}]")
            print_map(item, indent + 4)
          end)

        _ ->
          IO.puts("#{pad}#{key_str}: #{inspect(v)}")
      end
    end)
  end
end
