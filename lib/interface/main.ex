defmodule SuperCache do
  @moduledoc """
  SuperCache is an in-memory cache library built on top of ETS.

  ## Features
  - Auto-scales partitions based on CPU scheduler count.
  - Supports tuple and struct storage.
  - Provides lazy (buffered) writes for higher throughput.
  - Includes built-in Queue, Stack, KeyValue and Struct abstractions.

  ## Limitations
  - Single-node only (no cluster replication in the current version).
  - Supports tuple data natively; other types must be wrapped in a tuple.

  ## Quick start

      SuperCache.start!()
      # Push tuple to cache
      SuperCache.put!({:my_key, "some value"})
      # Get tuple by tuple style
      SuperCache.get!({:my_key, nil})   # => [{:my_key, "some value"}]
      # Get tuple by key/partition data.
      SuperCache.get_by_key_partition!(:my_key)
      SuperCache.stop()
  """

  require Logger

  alias SuperCache.{Bootstrap, Buffer, Config, Partition, Storage}

  ## Lifecycle ##

  @doc "Start SuperCache with default config.  Raises on error."
  @spec start!() :: :ok
  def start!(), do: Bootstrap.start!()

  @doc "Start SuperCache with `opts`.  Raises on error."
  @spec start!(keyword) :: :ok
  def start!(opts), do: Bootstrap.start!(opts)

  @doc "Start SuperCache with default config.  Returns `:ok | {:error, reason}`."
  @spec start() :: :ok | {:error, Exception.t()}
  def start(), do: safe(fn -> Bootstrap.start!() end)

  @doc "Start SuperCache with `opts`.  Returns `:ok | {:error, reason}`."
  @spec start(keyword) :: :ok | {:error, Exception.t()}
  def start(opts), do: safe(fn -> Bootstrap.start!(opts) end)

  @doc "Returns `true` when SuperCache has been started."
  @spec started?() :: boolean
  def started?(), do: Config.get_config(:started, false)

  @doc "Stop SuperCache and free all ETS memory."
  @spec stop() :: :ok
  def stop(), do: Bootstrap.stop()

  ## Write ##

  @doc """
  Store a tuple in the cache.  Raises on error.

  The tuple must be at least as large as `max(key_pos, partition_pos) + 1`.
  """
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data) do
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Logger.debug(fn -> "super_cache, put key=#{inspect(Config.get_key!(data))} partition=#{inspect(partition)}" end)
    Storage.put(data, partition)
  end

  @doc "Store a tuple.  Returns `true | {:error, reason}`."
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> put!(data) end)

  @doc """
  Enqueue a tuple for buffered (lazy) write.
  Use when immediate read-back is not required; improves write throughput.
  """
  @spec lazy_put(tuple) :: :ok
  def lazy_put(data) when is_tuple(data), do: Buffer.enqueue(data)

  ## Read ##

  @doc """
  Retrieve all records matching the key in `data`.  Raises on error.
  Returns a list of tuples (may be empty).
  """
  @spec get!(tuple) :: [tuple]
  def get!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Logger.debug(fn -> "super_cache, get key=#{inspect(key)} partition=#{inspect(partition)}" end)
    Storage.get(key, partition)
  end

  @doc "Retrieve records.  Returns `[tuple] | {:error, reason}`."
  @spec get(tuple) :: [tuple] | {:error, Exception.t()}
  def get(data), do: safe(fn -> get!(data) end)

  @doc "Retrieve records by explicit `key` and `partition` value."
  @spec get_by_key_partition!(any, any) :: [tuple]
  def get_by_key_partition!(key, partition_data) do
    Storage.get(key, Partition.get_partition(partition_data))
  end

  @doc "Retrieve records where `key_pos == partition_pos` (same value for both)."
  @spec get_same_key_partition!(any) :: [tuple]
  def get_same_key_partition!(key), do: get_by_key_partition!(key, key)

  @doc """
  Retrieve records matching an ETS match pattern (`:ets.match/2`).

  Pass `:_` as `partition_data` to scan all partitions.
  """
  @spec get_by_match!(any, tuple) :: [[any]]
  def get_by_match!(partition_data, pattern) when is_tuple(pattern) do
    reduce_partitions(partition_data, [], fn p, acc ->
      Storage.get_by_match(pattern, p) ++ acc
    end)
  end

  @doc "Scan all partitions with an ETS match pattern."
  @spec get_by_match!(tuple) :: [[any]]
  def get_by_match!(pattern) when is_tuple(pattern), do: get_by_match!(:_, pattern)

  @doc """
  Retrieve full objects matching an ETS match-object pattern (`:ets.match_object/2`).

  Pass `:_` as `partition_data` to scan all partitions.
  """
  @spec get_by_match_object!(any, tuple) :: [tuple]
  def get_by_match_object!(partition_data, pattern) when is_tuple(pattern) do
    reduce_partitions(partition_data, [], fn p, acc ->
      Storage.get_by_match_object(pattern, p) ++ acc
    end)
  end

  @doc "Scan all partitions with an ETS match-object pattern."
  @spec get_by_match_object!(tuple) :: [tuple]
  def get_by_match_object!(pattern) when is_tuple(pattern), do: get_by_match_object!(:_, pattern)

  ## Scan ##

  @doc """
  Fold over all records in a partition (or all partitions when `:_`).

  `fun/2` receives `(record, accumulator)` and must return the new accumulator.
  """
  @spec scan!(any, (any, any -> any), any) :: any
  def scan!(partition_data, fun, acc) when is_function(fun, 2) do
    reduce_partitions(partition_data, acc, fn p, result ->
      Storage.scan(fun, result, p)
    end)
  end

  @doc "Fold over all partitions."
  @spec scan!((any, any -> any), any) :: any
  def scan!(fun, acc) when is_function(fun, 2), do: scan!(:_, fun, acc)

  ## Delete ##

  @doc "Delete the record whose key and partition match those in `data`.  Raises on error."
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Storage.delete(key, partition)
    :ok
  end

  @doc "Delete all records from all partitions."
  @spec delete_all() :: :ok
  def delete_all() do
    Partition.get_all_partition()
    |> List.flatten()
    |> Enum.each(&Storage.delete_all/1)
  end

  @doc """
  Delete records matching `pattern` in a partition (or all partitions for `:_`).
  """
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    partition_data
    |> resolve_partitions()
    |> Enum.each(&Storage.delete_match(pattern, &1))
  end

  @doc "Delete matching records from all partitions."
  @spec delete_by_match!(tuple) :: :ok
  def delete_by_match!(pattern) when is_tuple(pattern), do: delete_by_match!(:_, pattern)

  @doc "Delete by explicit `key` and `partition_data`."
  @spec delete_by_key_partition!(any, any) :: true
  def delete_by_key_partition!(key, partition_data) do
    Storage.delete(key, Partition.get_partition(partition_data))
  end

  @doc "Delete where key and partition value are the same."
  @spec delete_same_key_partition!(any) :: true
  def delete_same_key_partition!(key), do: delete_by_key_partition!(key, key)

  ## Stats ##

  @doc "Return `[{partition, size}]` for every partition plus a `total:` keyword."
  @spec stats() :: keyword
  def stats() do
    entries =
      Partition.get_all_partition()
      |> List.flatten()
      |> Enum.map(&Storage.stats/1)

    total = Enum.reduce(entries, 0, fn {_, n}, acc -> acc + n end)
    entries ++ [total: total]
  end

  ## Private helpers ##

  # Resolves partition_data to a flat list of ETS table names.
  defp resolve_partitions(:_), do: List.flatten(Partition.get_all_partition())
  defp resolve_partitions(data), do: [Partition.get_partition(data)]

  # Reduce over resolved partitions, threading an accumulator.
  defp reduce_partitions(partition_data, acc, fun) do
    partition_data
    |> resolve_partitions()
    |> Enum.reduce(acc, fun)
  end

  # Wrap a raising function in a rescue, returning {:error, exception}.
  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
