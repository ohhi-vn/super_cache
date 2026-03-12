
# =============================================================================
# lib/super_cache.ex
#
# Unified public API.  In :local mode writes go directly to ETS.
# In :distributed mode writes are routed through Cluster.Router.
# =============================================================================
defmodule SuperCache do
  @moduledoc """
  High-throughput, in-memory cache backed by ETS.

  Works in both **local** (single-node) and **distributed** (cluster) modes.
  The mode is selected at startup via the `:cluster` option and is
  transparent to callers — the same API covers both.

  ## Setup

      # Local (single-node, default)
      SuperCache.start!(key_pos: 0, partition_pos: 0)

      # Distributed cluster
      SuperCache.start!(
        key_pos:            0,
        partition_pos:      0,
        cluster:            :distributed,
        replication_factor: 2,
        replication_mode:   :async,
        num_partition:      16
      )

      # Enable debug logging
      # in config.exs:
      # config :super_cache, debug_log: true

  ## Options

  | Option           | Type    | Default         | Description                          |
  |------------------|---------|-----------------|--------------------------------------|
  | `:key_pos`       | integer | `0`             | Tuple index used as the ETS key      |
  | `:partition_pos` | integer | `0`             | Tuple index used to select partition |
  | `:cluster`       | atom    | `:local`        | `:local` or `:distributed`           |
  | `:num_partition` | integer | scheduler count | Number of ETS partitions             |
  | `:table_type`    | atom    | `:set`          | ETS table type                       |

  ## Read modes (meaningful in distributed mode)

  | Mode       | Consistency        | Latency              |
  |------------|--------------------|----------------------|
  | `:local`   | Eventual (default) | Zero extra latency   |
  | `:primary` | Strong per key     | +1 RTT if non-primary|
  | `:quorum`  | Majority vote      | +1 RTT (parallel)    |

  ## Replication modes (distributed mode only)

  | Mode      | Guarantee          | Extra latency     |
  |-----------|--------------------|-------------------|
  | `:async`  | Eventual (default) | None              |
  | `:sync`   | At-least-once      | +1 RTT per write  |
  | `:strong` | Three-phase commit | +3 RTTs per write |

  ## Basic usage

      SuperCache.put!({:user, 1, "Alice"})
      SuperCache.get!({:user, 1, nil})
      # => [{:user, 1, "Alice"}]

      # Read modes (distributed)
      SuperCache.get!({:user, 1, nil}, read_mode: :primary)
      SuperCache.get!({:user, 1, nil}, read_mode: :quorum)

      SuperCache.delete!({:user, 1, nil})
      SuperCache.delete_all()

  ## Error handling

  `!`-suffix functions raise on error.  Non-bang variants return
  `{:error, exception}` instead and never raise.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Config, Partition, Storage, Buffer}
  alias SuperCache.Cluster.Router

  ## ── Lifecycle ────────────────────────────────────────────────────────────────

  @doc "Start SuperCache with default options (local, key_pos: 0, partition_pos: 0)."
  @spec start!() :: :ok
  def start!(), do: SuperCache.Bootstrap.start!()

  @doc "Start SuperCache with the given keyword options. Raises on invalid config."
  @spec start!(keyword) :: :ok
  def start!(opts), do: SuperCache.Bootstrap.start!(opts)

  @doc "Start SuperCache. Returns `:ok | {:error, exception}` instead of raising."
  @spec start() :: :ok | {:error, Exception.t()}
  def start(), do: safe(fn -> SuperCache.Bootstrap.start!() end)

  @doc "Start SuperCache with opts. Returns `:ok | {:error, exception}` instead of raising."
  @spec start(keyword) :: :ok | {:error, Exception.t()}
  def start(opts), do: safe(fn -> SuperCache.Bootstrap.start!(opts) end)

  @doc "Returns `true` when SuperCache is running and ready."
  @spec started?() :: boolean
  def started?(), do: Config.get_config(:started, false)

  @doc "Stop SuperCache and free all ETS memory."
  @spec stop() :: :ok
  def stop(), do: SuperCache.Bootstrap.stop()

  ## ── Write ────────────────────────────────────────────────────────────────────

  @doc """
  Store a tuple in the cache.

  In distributed mode, routes the write to the partition's primary node and
  replicates according to `:replication_mode`.  Raises on error.
  """
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data) do
    if distributed?() do
      Router.route_put!(data)
    else
      partition = data |> Config.get_partition!() |> Partition.get_partition()
      SuperCache.Log.debug(fn -> "super_cache, put key=#{inspect(Config.get_key!(data))} partition=#{inspect(partition)}" end)
      Storage.put(data, partition)
    end
  end

  @doc "Store a tuple. Returns `true | {:error, exception}` instead of raising."
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> put!(data) end)

  @doc """
  Enqueue a tuple for a buffered (lazy) write.

  Falls back to `put!/1` with a warning when `:strong` replication is active.
  """
  @spec lazy_put(tuple) :: :ok
  def lazy_put(data) when is_tuple(data) do
    if distributed?() and SuperCache.Cluster.Manager.replication_mode() == :strong do
      Logger.warning("super_cache, lazy_put called in :strong replication mode — falling back to put!")
      put!(data)
      :ok
    else
      Buffer.enqueue(data)
    end
  end

  ## ── Read ─────────────────────────────────────────────────────────────────────

  @doc """
  Retrieve all records whose key matches the key element of `data`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Examples

      SuperCache.get!({:user, 1, nil})
      SuperCache.get!({:user, 1, nil}, read_mode: :primary)
      SuperCache.get!({:user, 1, nil}, read_mode: :quorum)
  """
  @spec get!(tuple, keyword) :: [tuple]
  def get!(data, opts \\ []) when is_tuple(data) do
    if distributed?() do
      Router.route_get!(data, opts)
    else
      key       = Config.get_key!(data)
      partition = data |> Config.get_partition!() |> Partition.get_partition()
      SuperCache.Log.debug(fn -> "super_cache, get key=#{inspect(key)} partition=#{inspect(partition)}" end)
      Storage.get(key, partition)
    end
  end

  @doc "Retrieve records. Returns `[tuple] | {:error, exception}` instead of raising."
  @spec get(tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get(data, opts \\ []), do: safe(fn -> get!(data, opts) end)

  @doc """
  Retrieve records by explicit `key` and `partition_data`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec get_by_key_partition!(any, any, keyword) :: [tuple]
  def get_by_key_partition!(key, partition_data, opts \\ []) do
    if distributed?() do
      Router.route_get_by_key_partition!(key, partition_data, opts)
    else
      Storage.get(key, Partition.get_partition(partition_data))
    end
  end

  @doc "Non-raising variant of `get_by_key_partition!/3`."
  @spec get_by_key_partition(any, any, keyword) :: [tuple] | {:error, Exception.t()}
  def get_by_key_partition(key, partition_data, opts \\ []) do
    safe(fn -> get_by_key_partition!(key, partition_data, opts) end)
  end

  @doc "Equivalent to `get_by_key_partition!(key, key, opts)` — for tables where key_pos == partition_pos."
  @spec get_same_key_partition!(any, keyword) :: [tuple]
  def get_same_key_partition!(key, opts \\ []), do: get_by_key_partition!(key, key, opts)

  @doc "Non-raising variant of `get_same_key_partition!/2`."
  @spec get_same_key_partition(any, keyword) :: [tuple] | {:error, Exception.t()}
  def get_same_key_partition(key, opts \\ []), do: safe(fn -> get_same_key_partition!(key, opts) end)

  @doc """
  Retrieve records matching an ETS match pattern (binding lists).

  Pass `:_` as `partition_data` to scan all partitions.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec get_by_match!(any, tuple, keyword) :: [[any]]
  def get_by_match!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    if distributed?() do
      Router.route_get_by_match!(partition_data, pattern, opts)
    else
      reduce_partitions(partition_data, [], fn p, acc -> Storage.get_by_match(pattern, p) ++ acc end)
    end
  end

  @doc "Scan all partitions. Equivalent to `get_by_match!(:_, pattern, [])`."
  @spec get_by_match!(tuple) :: [[any]]
  def get_by_match!(pattern) when is_tuple(pattern), do: get_by_match!(:_, pattern, [])

  @doc "Non-raising variant of `get_by_match!/2`."
  @spec get_by_match(any, tuple, keyword) :: [[any]] | {:error, Exception.t()}
  def get_by_match(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    safe(fn -> get_by_match!(partition_data, pattern, opts) end)
  end

  @doc """
  Retrieve full records matching an ETS match-object pattern.

  Pass `:_` as `partition_data` to scan all partitions.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec get_by_match_object!(any, tuple, keyword) :: [tuple]
  def get_by_match_object!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    if distributed?() do
      Router.route_get_by_match_object!(partition_data, pattern, opts)
    else
      reduce_partitions(partition_data, [], fn p, acc -> Storage.get_by_match_object(pattern, p) ++ acc end)
    end
  end

  @doc "Scan all partitions. Equivalent to `get_by_match_object!(:_, pattern, [])`."
  @spec get_by_match_object!(tuple) :: [tuple]
  def get_by_match_object!(pattern) when is_tuple(pattern), do: get_by_match_object!(:_, pattern, [])

  @doc "Non-raising variant of `get_by_match_object!/2`."
  @spec get_by_match_object(any, tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get_by_match_object(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    safe(fn -> get_by_match_object!(partition_data, pattern, opts) end)
  end

  ## ── Scan ─────────────────────────────────────────────────────────────────────

  @doc """
  Fold over every record in a partition (or all when `:_`).

  Always reads from the local ETS table regardless of replication mode.
  """
  @spec scan!(any, (any, any -> any), any) :: any
  def scan!(partition_data, fun, acc) when is_function(fun, 2) do
    if distributed?() do
      Router.route_scan!(partition_data, fun, acc)
    else
      reduce_partitions(partition_data, acc, fn p, a -> Storage.scan(fun, a, p) end)
    end
  end

  @doc "Equivalent to `scan!(:_, fun, acc)`."
  @spec scan!((any, any -> any), any) :: any
  def scan!(fun, acc) when is_function(fun, 2), do: scan!(:_, fun, acc)

  @doc "Non-raising variant of `scan!/3`."
  @spec scan(any, (any, any -> any), any) :: any | {:error, Exception.t()}
  def scan(partition_data, fun, acc) when is_function(fun, 2) do
    safe(fn -> scan!(partition_data, fun, acc) end)
  end

  ## ── Delete ───────────────────────────────────────────────────────────────────

  @doc """
  Delete the record matching the key in `data`.

  Routes to the primary in distributed mode.
  """
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data) do
    if distributed?() do
      Router.route_delete!(data)
    else
      key       = Config.get_key!(data)
      partition = data |> Config.get_partition!() |> Partition.get_partition()
      Storage.delete(key, partition)
      :ok
    end
  end

  @doc "Non-raising variant of `delete!/1`."
  @spec delete(tuple) :: :ok | {:error, Exception.t()}
  def delete(data), do: safe(fn -> delete!(data) end)

  @doc """
  Delete all records from every partition.

  In distributed mode, issues one routed delete per partition to its primary.
  """
  @spec delete_all() :: :ok
  def delete_all() do
    if distributed?() do
      Router.route_delete_all()
    else
      Partition.get_all_partition() |> List.flatten() |> Enum.each(&Storage.delete_all/1)
    end
  end

  @doc """
  Delete records matching `pattern` in `partition_data` (or all partitions for `:_`).
  """
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    if distributed?() do
      Router.route_delete_match!(partition_data, pattern)
    else
      resolve_partitions(partition_data) |> Enum.each(&Storage.delete_match(pattern, &1))
    end
  end

  @doc "Delete from all partitions. Equivalent to `delete_by_match!(:_, pattern)`."
  @spec delete_by_match!(tuple) :: :ok
  def delete_by_match!(pattern) when is_tuple(pattern), do: delete_by_match!(:_, pattern)

  @doc "Non-raising variant of `delete_by_match!/2`."
  @spec delete_by_match(any, tuple) :: :ok | {:error, Exception.t()}
  def delete_by_match(partition_data, pattern) when is_tuple(pattern) do
    safe(fn -> delete_by_match!(partition_data, pattern) end)
  end

  @doc "Delete by explicit `key` and `partition_data`."
  @spec delete_by_key_partition!(any, any) :: :ok
  def delete_by_key_partition!(key, partition_data) do
    if distributed?() do
      Router.route_delete_by_key_partition!(key, partition_data)
    else
      Storage.delete(key, Partition.get_partition(partition_data))
      :ok
    end
  end

  @doc "Non-raising variant of `delete_by_key_partition!/2`."
  @spec delete_by_key_partition(any, any) :: :ok | {:error, Exception.t()}
  def delete_by_key_partition(key, partition_data) do
    safe(fn -> delete_by_key_partition!(key, partition_data) end)
  end

  @doc "Equivalent to `delete_by_key_partition!(key, key)` — for tables where key_pos == partition_pos."
  @spec delete_same_key_partition!(any) :: :ok
  def delete_same_key_partition!(key), do: delete_by_key_partition!(key, key)

  @doc "Non-raising variant of `delete_same_key_partition!/1`."
  @spec delete_same_key_partition(any) :: :ok | {:error, Exception.t()}
  def delete_same_key_partition(key), do: safe(fn -> delete_same_key_partition!(key) end)

  ## ── Stats ────────────────────────────────────────────────────────────────────

  @doc "Return local ETS record counts per partition plus `:total`."
  @spec stats() :: keyword
  def stats() do
    entries = Partition.get_all_partition() |> List.flatten() |> Enum.map(&Storage.stats/1)
    total   = Enum.reduce(entries, 0, fn {_, n}, acc -> acc + n end)
    entries ++ [total: total]
  end

  @doc """
  Return an aggregated cluster-wide statistics map.

  In local mode wraps `stats/0` in the same map format for a uniform interface.
  In distributed mode gathers per-node counts via `:erpc`.
  """
  @spec cluster_stats() :: map
  def cluster_stats() do
    if distributed?() do
      SuperCache.Cluster.Stats.cluster() |> Map.merge(gather_node_stats())
    else
      base  = stats()
      total = Keyword.get(base, :total, 0)

      %{
        nodes:              [node()],
        node_count:         1,
        replication_factor: 1,
        replication_mode:   :none,
        num_partitions:     Partition.get_num_partition(),
        total_records:      total,
        node_stats:         %{node() => [partition_count: length(base) - 1, record_count: total]},
        unreachable_nodes:  []
      }
    end
  end

  ## ── Private ──────────────────────────────────────────────────────────────────

  @doc false
  def distributed?(), do: Config.get_config(:cluster, :local) == :distributed

  defp resolve_partitions(:_),   do: List.flatten(Partition.get_all_partition())
  defp resolve_partitions(data), do: [Partition.get_partition(data)]

  defp reduce_partitions(partition_data, acc, fun) do
    partition_data |> resolve_partitions() |> Enum.reduce(acc, fun)
  end

  defp gather_node_stats() do
    live = SuperCache.Cluster.Manager.live_nodes()

    {node_stats, unreachable} =
      live
      |> Task.async_stream(
        fn n ->
          try do
            s     = :erpc.call(n, __MODULE__, :stats, [], 5_000)
            total = Keyword.get(s, :total, 0)
            {n, [partition_count: length(s) - 1, record_count: total]}
          catch
            _, reason -> {n, {:unreachable, reason}}
          end
        end,
        timeout: 8_000, on_timeout: :kill_task
      )
      |> Enum.reduce({%{}, []}, fn
        {:ok, {n, {:unreachable, _}}}, {s, bad} -> {s, [n | bad]}
        {:ok, {n, info}},             {s, bad}  -> {Map.put(s, n, info), bad}
        {:exit, _},                   {s, bad}  -> {s, bad}
      end)

    %{node_stats: node_stats, unreachable_nodes: unreachable}
  end

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
