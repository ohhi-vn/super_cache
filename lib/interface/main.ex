defmodule SuperCache do
  @moduledoc """
  SuperCache is a high-throughput, in-memory cache built on top of ETS.

  ## Architecture

  Data is spread across multiple ETS partitions — one per CPU scheduler by
  default.  Partition assignment is determined by hashing the *partition value*
  of each tuple (configured via `:partition_pos`).  This eliminates
  cross-partition contention so concurrent reads and writes scale linearly
  with CPU count.

  ## Configuration

  | Option           | Type    | Default          | Description                          |
  |------------------|---------|------------------|--------------------------------------|
  | `:key_pos`       | integer | `0`              | Tuple index used as the ETS key      |
  | `:partition_pos` | integer | `0`              | Tuple index used to select partition |
  | `:num_partition` | integer | scheduler count  | Number of ETS partitions             |
  | `:table_type`    | atom    | `:set`           | ETS table type                       |
  | `:table_prefix`  | string  | `"SuperCache…"`  | Prefix for ETS table atom names      |

  ## Data model

  Every value stored in SuperCache **must be a tuple**.  The element at
  `:key_pos` is the ETS lookup key; the element at `:partition_pos` selects
  which partition the record lives in.  Both positions may point to the same
  element.

      # key_pos: 0, partition_pos: 1
      # key = :user, partition = 42
      {:user, 42, "Alice", :admin}

  ## Quick start

      SuperCache.start!()

      # Store a record
      SuperCache.put!({:product, "sku-1", "Widget", 9.99})

      # Fetch by key — returns a list (may contain multiple records for :bag tables)
      [{:product, "sku-1", "Widget", 9.99}] = SuperCache.get!({:product, "sku-1", nil, nil})

      # Pattern scan across all partitions
      SuperCache.get_by_match_object!({:product, :_, :_, :"$1"})

      # Delete
      SuperCache.delete!({:product, "sku-1", nil, nil})

      SuperCache.stop()

  ## Lazy writes

  `lazy_put/1` enqueues the record into a per-scheduler write buffer instead
  of writing to ETS immediately.  Use it when write throughput matters more
  than immediate read-back consistency:

      SuperCache.lazy_put({:event, "evt-123", %{type: :click}})

  ## Cluster mode

  For distributed deployments use `SuperCache.Cluster.Bootstrap.start!/1`
  and the `SuperCache.Distributed` API instead.  The single-node API shown
  here does not replicate writes.

  ## Limitations

  - Only tuples are supported natively.  Wrap other types: `{:my_key, value}`.
  - `:bag` and `:duplicate_bag` tables allow multiple records per key; `:set`
    and `:ordered_set` do not.
  - There is no TTL / expiry mechanism; eviction must be done explicitly.
  """

  require Logger

  alias SuperCache.{Bootstrap, Buffer, Config, Partition, Storage}

  ## Lifecycle ──────────────────────────────────────────────────────────────────

  @doc """
  Start SuperCache with default configuration.

  Equivalent to `start!([key_pos: 0, partition_pos: 0])`.

  Raises `ArgumentError` on invalid options or if required options are missing.
  """
  @spec start!() :: :ok
  def start!(), do: Bootstrap.start!()

  @doc """
  Start SuperCache with the given keyword options.

  Required: `:key_pos`, `:partition_pos`.

  ## Examples

      # Both key and partition derived from element 0
      SuperCache.start!(key_pos: 0, partition_pos: 0)

      # Key at index 1, partition at index 0 (e.g. {:namespace, :id, value})
      SuperCache.start!(key_pos: 1, partition_pos: 0, num_partition: 16)

      # Ordered set for range queries
      SuperCache.start!(key_pos: 0, partition_pos: 0, table_type: :ordered_set)
  """
  @spec start!(keyword) :: :ok
  def start!(opts), do: Bootstrap.start!(opts)

  @doc """
  Start SuperCache with default configuration.

  Returns `:ok` on success or `{:error, exception}` instead of raising.
  """
  @spec start() :: :ok | {:error, Exception.t()}
  def start(), do: safe(fn -> Bootstrap.start!() end)

  @doc """
  Start SuperCache with the given keyword options.

  Returns `:ok` on success or `{:error, exception}` instead of raising.
  """
  @spec start(keyword) :: :ok | {:error, Exception.t()}
  def start(opts), do: safe(fn -> Bootstrap.start!(opts) end)

  @doc """
  Returns `true` when SuperCache has been started and is ready to serve
  requests.

  ## Example

      SuperCache.started?()   # => false
      SuperCache.start!()
      SuperCache.started?()   # => true
  """
  @spec started?() :: boolean
  def started?(), do: Config.get_config(:started, false)

  @doc """
  Stop SuperCache and free all ETS memory.

  Safe to call even if SuperCache was never started.
  """
  @spec stop() :: :ok
  def stop(), do: Bootstrap.stop()

  ## Write ──────────────────────────────────────────────────────────────────────

  @doc """
  Store a tuple in the cache.

  The tuple must have at least `max(key_pos, partition_pos) + 1` elements.
  For `:set` tables, inserting a record with a key that already exists
  **overwrites** the previous record.

  Raises `SuperCache.Config` if the tuple is too small.

  ## Examples

      # Simple key-value style
      SuperCache.put!({:session, "tok-abc", %{user_id: 1, role: :admin}})

      # Multiple fields; key at 0, partition at 1
      SuperCache.put!({:order, :eu_west, "ord-9", :pending, 120.00})
  """
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data) do
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Logger.debug(fn -> "super_cache, put key=#{inspect(Config.get_key!(data))} partition=#{inspect(partition)}" end)
    Storage.put(data, partition)
  end

  @doc """
  Store a tuple.

  Returns `true` on success or `{:error, exception}` instead of raising.
  """
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> put!(data) end)

  @doc """
  Enqueue a tuple for a buffered (lazy) write.

  The record is placed in the current scheduler's write buffer and flushed
  to ETS asynchronously.  Use when high write throughput is required and
  the caller does not need to read the record back immediately.

  ## Example

      # High-volume event ingestion — fire-and-forget
      for event <- events do
        SuperCache.lazy_put({:event, event.id, event.type, event.ts})
      end
  """
  @spec lazy_put(tuple) :: :ok
  def lazy_put(data) when is_tuple(data), do: Buffer.enqueue(data)

  ## Read ───────────────────────────────────────────────────────────────────────

  @doc """
  Retrieve all records whose key matches the key element of `data`.

  Returns a list of tuples.  The list is empty when no record exists.
  For `:set` tables the list has at most one element; for `:bag` tables
  it may have many.

  ## Examples

      SuperCache.put!({:user, 1, "Alice"})
      SuperCache.get!({:user, 1, nil})
      # => [{:user, 1, "Alice"}]

      SuperCache.get!({:user, 999, nil})
      # => []
  """
  @spec get!(tuple) :: [tuple]
  def get!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Logger.debug(fn -> "super_cache, get key=#{inspect(key)} partition=#{inspect(partition)}" end)
    Storage.get(key, partition)
  end

  @doc """
  Retrieve records.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get(tuple) :: [tuple] | {:error, Exception.t()}
  def get(data), do: safe(fn -> get!(data) end)

  @doc """
  Retrieve records by explicit `key` and `partition_data`.

  Useful when the key and partition values are known without constructing
  a full data tuple.

  ## Example

      SuperCache.put!({:item, :eu, "i-1", 42})
      SuperCache.get_by_key_partition!(:item, :eu)
      # => [{:item, :eu, "i-1", 42}]
  """
  @spec get_by_key_partition!(any, any) :: [tuple]
  def get_by_key_partition!(key, partition_data) do
    Storage.get(key, Partition.get_partition(partition_data))
  end

  @doc """
  Retrieve records where the key and partition value are the same term.

  Equivalent to `get_by_key_partition!(key, key)`.  Convenient for the
  common pattern where `:key_pos == :partition_pos`.

  ## Example

      SuperCache.start!(key_pos: 0, partition_pos: 0)
      SuperCache.put!({:config, :timeout, 5_000})
      SuperCache.get_same_key_partition!(:config)
      # => [{:config, :timeout, 5_000}]
  """
  @spec get_same_key_partition!(any) :: [tuple]
  def get_same_key_partition!(key), do: get_by_key_partition!(key, key)

  @doc """
  Retrieve records matching an ETS match pattern using `:ets.match/2`.

  Returns a list of binding lists (one per matched record), **not** full
  tuples.  Use `get_by_match_object!/2` when you need the full records.

  Pass `:_` as `partition_data` to scan all partitions.

  ## Examples

      SuperCache.put!({:order, :eu, "o-1", :pending})
      SuperCache.put!({:order, :eu, "o-2", :shipped})

      # Extract the order id and status from all :eu partition records
      SuperCache.get_by_match!(:eu, {:order, :eu, :"$1", :"$2"})
      # => [["o-1", :pending], ["o-2", :shipped]]

      # Scan every partition
      SuperCache.get_by_match!({:order, :_, :"$1", :pending})
  """
  @spec get_by_match!(any, tuple) :: [[any]]
  def get_by_match!(partition_data, pattern) when is_tuple(pattern) do
    reduce_partitions(partition_data, [], fn p, acc ->
      Storage.get_by_match(pattern, p) ++ acc
    end)
  end

  @doc """
  Scan all partitions with an ETS match pattern.

  Equivalent to `get_by_match!(:_, pattern)`.
  """
  @spec get_by_match!(tuple) :: [[any]]
  def get_by_match!(pattern) when is_tuple(pattern), do: get_by_match!(:_, pattern)

  @doc """
  Retrieve full records matching an ETS match-object pattern.

  Returns a list of full tuples.  Pass `:_` as `partition_data` to scan
  all partitions.

  ## Examples

      SuperCache.put!({:product, :eu, "p-1", "Widget", 9.99})
      SuperCache.put!({:product, :eu, "p-2", "Gadget", 24.99})

      # All products in the :eu partition
      SuperCache.get_by_match_object!(:eu, {:product, :eu, :_, :_, :_})
      # => [{:product, :eu, "p-1", "Widget", 9.99},
      #     {:product, :eu, "p-2", "Gadget", 24.99}]

      # Find all products priced above some threshold — use a guard spec instead
      # of match_object for comparisons; this example shows the pattern form.
      SuperCache.get_by_match_object!({:product, :_, :_, :_, :_})
  """
  @spec get_by_match_object!(any, tuple) :: [tuple]
  def get_by_match_object!(partition_data, pattern) when is_tuple(pattern) do
    reduce_partitions(partition_data, [], fn p, acc ->
      Storage.get_by_match_object(pattern, p) ++ acc
    end)
  end

  @doc """
  Scan all partitions with an ETS match-object pattern.

  Equivalent to `get_by_match_object!(:_, pattern)`.
  """
  @spec get_by_match_object!(tuple) :: [tuple]
  def get_by_match_object!(pattern) when is_tuple(pattern), do: get_by_match_object!(:_, pattern)

  ## Scan ───────────────────────────────────────────────────────────────────────

  @doc """
  Fold over every record in a partition (or all partitions when `:_`).

  `fun/2` receives `(record, accumulator)` and must return the new
  accumulator.  Records are visited in an unspecified order.

  ## Examples

      # Sum a numeric field across all records in one partition
      SuperCache.put!({:metric, :host1, :latency, 42})
      SuperCache.put!({:metric, :host1, :latency, 58})

      SuperCache.scan!(:host1, fn {_, _, _, v}, acc -> acc + v end, 0)
      # => 100

      # Count all records across every partition
      SuperCache.scan!(fn _record, acc -> acc + 1 end, 0)
  """
  @spec scan!(any, (any, any -> any), any) :: any
  def scan!(partition_data, fun, acc) when is_function(fun, 2) do
    reduce_partitions(partition_data, acc, fn p, result ->
      Storage.scan(fun, result, p)
    end)
  end

  @doc """
  Fold over all partitions.

  Equivalent to `scan!(:_, fun, acc)`.
  """
  @spec scan!((any, any -> any), any) :: any
  def scan!(fun, acc) when is_function(fun, 2), do: scan!(:_, fun, acc)

  ## Delete ─────────────────────────────────────────────────────────────────────

  @doc """
  Delete the record whose key and partition match those in `data`.

  For `:bag` tables all records with the matching key are removed.

  ## Example

      SuperCache.put!({:session, "tok-1", :active})
      SuperCache.delete!({:session, "tok-1", nil})
      SuperCache.get!({:session, "tok-1", nil})   # => []
  """
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    partition = data |> Config.get_partition!() |> Partition.get_partition()
    Storage.delete(key, partition)
    :ok
  end

  @doc """
  Delete all records from every partition.

  This is a local-only operation.  In cluster mode use
  `SuperCache.Distributed.delete_all/0` which routes the delete to every
  primary node.
  """
  @spec delete_all() :: :ok
  def delete_all() do
    Partition.get_all_partition()
    |> List.flatten()
    |> Enum.each(&Storage.delete_all/1)
  end

  @doc """
  Delete records matching `pattern` in `partition_data` (or all partitions
  for `:_`).

  Uses `:ets.match_delete/2` semantics — only the tuple structure is matched,
  not guard expressions.

  ## Examples

      # Remove all :expired sessions from the :eu partition
      SuperCache.delete_by_match!(:eu, {:session, :_, :expired})

      # Remove a specific record shape from every partition
      SuperCache.delete_by_match!({:tmp_lock, :_, :_})
  """
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    partition_data
    |> resolve_partitions()
    |> Enum.each(&Storage.delete_match(pattern, &1))
  end

  @doc """
  Delete records matching `pattern` from all partitions.

  Equivalent to `delete_by_match!(:_, pattern)`.
  """
  @spec delete_by_match!(tuple) :: :ok
  def delete_by_match!(pattern) when is_tuple(pattern), do: delete_by_match!(:_, pattern)

  @doc """
  Delete by explicit `key` and `partition_data`.

  ## Example

      SuperCache.delete_by_key_partition!("tok-1", :eu)
  """
  @spec delete_by_key_partition!(any, any) :: true
  def delete_by_key_partition!(key, partition_data) do
    Storage.delete(key, Partition.get_partition(partition_data))
  end

  @doc """
  Delete where the key and partition value are the same term.

  Equivalent to `delete_by_key_partition!(key, key)`.
  """
  @spec delete_same_key_partition!(any) :: true
  def delete_same_key_partition!(key), do: delete_by_key_partition!(key, key)

  ## Stats ──────────────────────────────────────────────────────────────────────

  @doc """
  Return the record count for each partition plus a `:total` summary.

  ## Example

      SuperCache.stats()
      # => [
      #   {:"SuperCache.Storage.Ets_0", 1024},
      #   {:"SuperCache.Storage.Ets_1", 998},
      #   ...
      #   total: 8176
      # ]
  """
  @spec stats() :: keyword
  def stats() do
    entries =
      Partition.get_all_partition()
      |> List.flatten()
      |> Enum.map(&Storage.stats/1)

    total = Enum.reduce(entries, 0, fn {_, n}, acc -> acc + n end)
    entries ++ [total: total]
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp resolve_partitions(:_),   do: List.flatten(Partition.get_all_partition())
  defp resolve_partitions(data), do: [Partition.get_partition(data)]

  defp reduce_partitions(partition_data, acc, fun) do
    partition_data
    |> resolve_partitions()
    |> Enum.reduce(acc, fun)
  end

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
