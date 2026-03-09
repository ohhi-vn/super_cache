defmodule SuperCache.Distributed do
  @moduledoc """
  Cluster-aware public API for SuperCache.

  This module is the distributed drop-in replacement for the single-node
  `SuperCache` API.  Every write is routed to the partition's **primary node**
  automatically; reads default to the local ETS table for lowest latency.

  ## Routing

  Partition ownership is determined by `SuperCache.Cluster.Manager`, which
  hashes each partition value across the sorted list of live nodes.  When the
  caller's node is the primary, the write is applied locally and then
  replicated.  When it is not, the write is forwarded via `:erpc` to the
  primary, which then applies and replicates it.

  ```
  Any node              Primary node         Replica nodes
     |-- put!({…}) ------> local_put          apply_op (async)
     |                          |-----------> replica 1
     |                          |-----------> replica 2
  ```

  See `SuperCache.Cluster.Router` for the full forwarding logic and the
  design rule that prevents forwarding cycles.

  ## Read modes

  | Mode       | Consistency               | Latency              |
  |------------|---------------------------|----------------------|
  | `:local`   | Eventual                  | Zero extra latency   |
  | `:primary` | Strong (per key)          | +1 RTT if non-primary|
  | `:quorum`  | Majority vote             | +1 RTT (parallel)    |

  `:local` is the default.  Use `:primary` or `:quorum` when you need to
  read your own writes from any node in the cluster.

  ## Replication modes

  Configured via `:replication_mode` in `Bootstrap.start!/1`:

  | Mode      | Guarantee              | Extra latency    |
  |-----------|------------------------|------------------|
  | `:async`  | Eventual (default)     | None             |
  | `:sync`   | At-least-once delivery | +1 RTT per write |
  | `:strong` | Three-phase commit     | +3 RTTs per write|

  When `:replication_mode` is `:strong`, every write (including
  `put!`, `delete!`, `delete_by_match!`, and `delete_all/0`) is
  committed through `SuperCache.Cluster.ThreePhaseCommit` before the
  function returns.  The coordinator runs on the **primary node**;
  if the caller is not the primary the entire operation is forwarded
  to the primary via `:erpc` first.

  ## Setup

  ```elixir
      SuperCache.Cluster.Bootstrap.start!(
        key_pos:            0,
        partition_pos:      0,
        cluster:            :distributed,
        replication_factor: 2,
        replication_mode:   :async,
        num_partition:      16
      )
  ```

  ## Basic usage

  ```elixir
      alias SuperCache.Distributed, as: Cache

      # Write — routed to the correct primary automatically
      Cache.put!({:user, 1, "Alice", :admin})

      # Buffered write — lower latency, eventual consistency
      Cache.lazy_put({:user, 2, "Bob", :member})

      # Eventual read from local replica (fastest)
      Cache.get!({:user, 1, nil, nil})

      # Strong read — forwarded to primary if this node is not the primary
      Cache.get!({:user, 1, nil, nil}, read_mode: :primary)

      # Quorum read — majority of replicas must agree
      Cache.get!({:user, 1, nil, nil}, read_mode: :quorum)

      # Retrieve by explicit key + partition
      Cache.get_by_key_partition!(:user, 1)

      # Retrieve where key == partition (key_pos == partition_pos)
      Cache.get_same_key_partition!(:user)

      # Pattern match — returns binding lists
      Cache.get_by_match!(:eu, {:order, :eu, :"$1", :pending})

      # Pattern match across all partitions
      Cache.get_by_match!({:order, :_, :"$1", :pending})

      # Full tuple pattern match
      Cache.get_by_match_object!(:eu, {:product, :eu, :_, :_, :_})
      Cache.get_by_match_object!({:product, :_, :_, :_, :_})

      # Fold over a partition (or all partitions)
      Cache.scan!(:eu, fn {_, _, _, price}, acc -> acc + price end, 0)
      Cache.scan!(fn _rec, acc -> acc + 1 end, 0)

      # Delete
      Cache.delete!({:user, 1, nil, nil})

      # Pattern delete across all partitions
      Cache.delete_by_match!(:_, {:user, :_, :_, :guest})

      # Full cluster wipe (one routed call per partition)
      Cache.delete_all()

      # Cluster stats — aggregated view across all live nodes
      Cache.cluster_stats()
  ```

  ## Error handling

  The `!`-suffix functions raise on error.  The non-bang variants return
  `{:error, exception}` instead:

  ```elixir
      case Cache.put(record) do
        true             -> :ok
        {:error, reason} -> :failed
      end
  ```
  """

  require Logger

  alias SuperCache.{Config, Partition}
  alias SuperCache.Cluster.{Router, Manager, Replicator, ThreePhaseCommit}

  ## ── Lifecycle helpers ────────────────────────────────────────────────────────

  @doc """
  Returns `true` when SuperCache has been started and is ready on this node.

  ## Example

      Cache.started?()   # => true
  """
  @spec started?() :: boolean
  def started?(), do: Config.get_config(:started, false)

  ## ── Write ────────────────────────────────────────────────────────────────────

  @doc """
  Store a tuple.  Routes to the partition's primary node if needed.

  Under `:strong` replication mode the write is committed through a
  three-phase commit before this function returns, giving linearisable
  per-key consistency.

  Raises on error.  See module docs for routing and replication behaviour.

  ## Example

      Cache.put!({:order, "ord-1", :eu, :pending, 99.00})
  """
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data), do: Router.route_put!(data)

  @doc """
  Store a tuple.

  Returns `true | {:error, exception}` instead of raising.
  """
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> Router.route_put!(data) end)

  @doc """
  Enqueue a tuple for a buffered (lazy) write on the current node.

  The record is written to the local write buffer and flushed to ETS
  asynchronously.  The buffer flush **does not route** to the primary — use
  this only when the calling node IS the primary for the relevant partition,
  or when eventual-consistency writes directly to a local replica are
  acceptable.

  Not compatible with `:strong` replication mode.

  ## Example

      # High-volume event ingestion on the primary node
      for event <- events do
        Cache.lazy_put({:event, event.id, event.type, event.ts})
      end
  """
  @spec lazy_put(tuple) :: :ok
  def lazy_put(data) when is_tuple(data) do
    if Manager.replication_mode() == :strong do
      Logger.warning(
        "super_cache, distributed, lazy_put called with :strong replication mode — " <>
          "falling back to routed put!"
      )

      Router.route_put!(data)
      :ok
    else
      SuperCache.Buffer.enqueue(data)
    end
  end

  ## ── Read ─────────────────────────────────────────────────────────────────────

  @doc """
  Retrieve records matching the key in `data`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Examples

      # Default: read from local ETS (may be stale on a replica)
      Cache.get!({:user, 1, nil})

      # Strong: always read from the primary
      Cache.get!({:user, 1, nil}, read_mode: :primary)

      # Quorum: at least ⌈(replicas+1)/2⌉ nodes must return the same value
      Cache.get!({:user, 1, nil}, read_mode: :quorum)
  """
  @spec get!(tuple, keyword) :: [tuple]
  def get!(data, opts \\ []) when is_tuple(data), do: Router.route_get!(data, opts)

  @doc """
  Retrieve records.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get(tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get(data, opts \\ []), do: safe(fn -> Router.route_get!(data, opts) end)

  @doc """
  Retrieve records by explicit `key` and `partition_data`.

  By default reads from the local replica.  Pass `read_mode: :primary` or
  `read_mode: :quorum` for stronger consistency.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      Cache.put!({:item, :eu, "i-1", 42})
      Cache.get_by_key_partition!(:item, :eu)
      # => [{:item, :eu, "i-1", 42}]

      Cache.get_by_key_partition!(:item, :eu, read_mode: :primary)
  """
  @spec get_by_key_partition!(any, any, keyword) :: [tuple]
  def get_by_key_partition!(key, partition_data, opts \\ []) do
    Router.route_get_by_key_partition!(key, partition_data, opts)
  end

  @doc """
  Retrieve records by explicit key and partition value.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get_by_key_partition(any, any, keyword) :: [tuple] | {:error, Exception.t()}
  def get_by_key_partition(key, partition_data, opts \\ []) do
    safe(fn -> Router.route_get_by_key_partition!(key, partition_data, opts) end)
  end

  @doc """
  Retrieve records where the key and partition value are the same term.

  Equivalent to `get_by_key_partition!(key, key, opts)`.  Convenient for
  the common pattern where `:key_pos == :partition_pos`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      Cache.start!(key_pos: 0, partition_pos: 0)
      Cache.put!({:config, :timeout, 5_000})
      Cache.get_same_key_partition!(:config)
      # => [{:config, :timeout, 5_000}]
  """
  @spec get_same_key_partition!(any, keyword) :: [tuple]
  def get_same_key_partition!(key, opts \\ []) do
    get_by_key_partition!(key, key, opts)
  end

  @doc """
  Retrieve records where key == partition.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get_same_key_partition(any, keyword) :: [tuple] | {:error, Exception.t()}
  def get_same_key_partition(key, opts \\ []) do
    safe(fn -> get_by_key_partition!(key, key, opts) end)
  end

  @doc """
  Retrieve records matching an ETS match pattern using `:ets.match/2`.

  Returns a list of binding lists (one per matched record), **not** full
  tuples.  Use `get_by_match_object!/2` when you need full records.

  Pass `:_` as `partition_data` to scan all local partitions.

  For `:primary` or `:quorum` read modes the scan is fanned out to the
  relevant nodes and results are merged locally.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Examples

      Cache.put!({:order, :eu, "o-1", :pending})
      Cache.put!({:order, :eu, "o-2", :shipped})

      # Extract order id and status from all :eu records
      Cache.get_by_match!(:eu, {:order, :eu, :"$1", :"$2"})
      # => [["o-1", :pending], ["o-2", :shipped]]

      # Scan every partition
      Cache.get_by_match!({:order, :_, :"$1", :pending})
  """
  @spec get_by_match!(any, tuple, keyword) :: [[any]]
  def get_by_match!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    Router.route_get_by_match!(partition_data, pattern, opts)
  end

  @doc """
  Scan all partitions with an ETS match pattern.

  Equivalent to `get_by_match!(:_, pattern, opts)`.
  """
  @spec get_by_match!(tuple) :: [[any]]
  def get_by_match!(pattern) when is_tuple(pattern) do
    get_by_match!(:_, pattern, [])
  end

  @doc """
  Retrieve records by match pattern.

  Returns `[[any]] | {:error, exception}` instead of raising.
  """
  @spec get_by_match(any, tuple, keyword) :: [[any]] | {:error, Exception.t()}
  def get_by_match(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    safe(fn -> Router.route_get_by_match!(partition_data, pattern, opts) end)
  end

  @doc """
  Retrieve full records matching an ETS match-object pattern.

  Returns a list of full tuples.  Pass `:_` as `partition_data` to scan
  all partitions.

  For `:primary` or `:quorum` read modes the scan is fanned out and results
  are merged.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Examples

      Cache.put!({:product, :eu, "p-1", "Widget", 9.99})
      Cache.put!({:product, :eu, "p-2", "Gadget", 24.99})

      # All products in the :eu partition
      Cache.get_by_match_object!(:eu, {:product, :eu, :_, :_, :_})
      # => [{:product, :eu, "p-1", "Widget", 9.99},
      #     {:product, :eu, "p-2", "Gadget", 24.99}]

      # All products across every partition
      Cache.get_by_match_object!({:product, :_, :_, :_, :_})
  """
  @spec get_by_match_object!(any, tuple, keyword) :: [tuple]
  def get_by_match_object!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    Router.route_get_by_match_object!(partition_data, pattern, opts)
  end

  @doc """
  Scan all partitions with an ETS match-object pattern.

  Equivalent to `get_by_match_object!(:_, pattern, opts)`.
  """
  @spec get_by_match_object!(tuple) :: [tuple]
  def get_by_match_object!(pattern) when is_tuple(pattern) do
    get_by_match_object!(:_, pattern, [])
  end

  @doc """
  Retrieve full records by match-object pattern.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get_by_match_object(any, tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get_by_match_object(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    safe(fn -> Router.route_get_by_match_object!(partition_data, pattern, opts) end)
  end

  ## ── Scan ─────────────────────────────────────────────────────────────────────

  @doc """
  Fold over every record in a partition (or all partitions when `:_`).

  `fun/2` receives `(record, accumulator)` and must return the new
  accumulator.  Always reads from the **local** ETS table regardless of
  replication mode — use `get_by_match_object!/2` with `read_mode: :primary`
  for consistent range scans.

  ## Examples

      # Sum a price field across all records in the :eu partition
      Cache.scan!(:eu, fn {_, _, _, price}, acc -> acc + price end, 0)

      # Count records across every partition
      Cache.scan!(fn _rec, acc -> acc + 1 end, 0)
  """
  @spec scan!(any, (any, any -> any), any) :: any
  def scan!(partition_data, fun, acc) when is_function(fun, 2) do
    Router.route_scan!(partition_data, fun, acc)
  end

  @doc """
  Fold over all partitions.

  Equivalent to `scan!(:_, fun, acc)`.
  """
  @spec scan!((any, any -> any), any) :: any
  def scan!(fun, acc) when is_function(fun, 2), do: scan!(:_, fun, acc)

  @doc """
  Fold over a partition (or all partitions).

  Returns `result | {:error, exception}` instead of raising.
  """
  @spec scan(any, (any, any -> any), any) :: any | {:error, Exception.t()}
  def scan(partition_data, fun, acc) when is_function(fun, 2) do
    safe(fn -> Router.route_scan!(partition_data, fun, acc) end)
  end

  ## ── Delete ───────────────────────────────────────────────────────────────────

  @doc """
  Delete the record matching `data`.  Routes to the primary for this key.

  Under `:strong` replication, deletion is committed through 3PC before
  returning.

  ## Example

      Cache.delete!({:session, "tok-xyz", nil})
  """
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data), do: Router.route_delete!(data)

  @doc """
  Delete a record.

  Returns `:ok | {:error, exception}` instead of raising.
  """
  @spec delete(tuple) :: :ok | {:error, Exception.t()}
  def delete(data), do: safe(fn -> Router.route_delete!(data) end)

  @doc """
  Delete all records across every partition on every node.

  Each partition delete is routed to its own primary, so this issues
  `num_partition` forwarded calls concurrently.  Under `:strong` replication
  each partition deletion goes through 3PC independently.
  """
  @spec delete_all() :: :ok
  def delete_all(), do: Router.route_delete_all()

  @doc """
  Delete records matching `pattern` in `partition_data` (or all partitions
  when `:_`).  Routes to the correct primary per partition.

  Under `:strong` replication each affected partition is committed
  through 3PC before the corresponding delete is acknowledged.

  ## Examples

      # Remove all expired sessions from the :eu partition
      Cache.delete_by_match!(:eu, {:session, :_, :expired})

      # Wipe a record shape cluster-wide
      Cache.delete_by_match!(:_, {:tmp_lock, :_, :_})
  """
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    Router.route_delete_match!(partition_data, pattern)
  end

  @doc """
  Delete records matching `pattern` from all partitions.

  Equivalent to `delete_by_match!(:_, pattern)`.
  """
  @spec delete_by_match!(tuple) :: :ok
  def delete_by_match!(pattern) when is_tuple(pattern), do: delete_by_match!(:_, pattern)

  @doc """
  Delete records matching pattern.

  Returns `:ok | {:error, exception}` instead of raising.
  """
  @spec delete_by_match(any, tuple) :: :ok | {:error, Exception.t()}
  def delete_by_match(partition_data, pattern) when is_tuple(pattern) do
    safe(fn -> Router.route_delete_match!(partition_data, pattern) end)
  end

  @doc """
  Delete by explicit `key` and `partition_data`.  Routes to the primary.

  Under `:strong` replication the deletion is committed through 3PC.

  ## Example

      Cache.delete_by_key_partition!("tok-abc", :eu)
  """
  @spec delete_by_key_partition!(any, any) :: :ok
  def delete_by_key_partition!(key, partition_data) do
    Router.route_delete_by_key_partition!(key, partition_data)
  end

  @doc """
  Delete by key and partition value.

  Returns `:ok | {:error, exception}` instead of raising.
  """
  @spec delete_by_key_partition(any, any) :: :ok | {:error, Exception.t()}
  def delete_by_key_partition(key, partition_data) do
    safe(fn -> Router.route_delete_by_key_partition!(key, partition_data) end)
  end

  @doc """
  Delete where the key and partition value are the same term.

  Equivalent to `delete_by_key_partition!(key, key)`.

  ## Example

      Cache.delete_same_key_partition!(:config)
  """
  @spec delete_same_key_partition!(any) :: :ok
  def delete_same_key_partition!(key), do: delete_by_key_partition!(key, key)

  @doc """
  Delete where key == partition.

  Returns `:ok | {:error, exception}` instead of raising.
  """
  @spec delete_same_key_partition(any) :: :ok | {:error, Exception.t()}
  def delete_same_key_partition(key) do
    safe(fn -> delete_by_key_partition!(key, key) end)
  end

  ## ── Stats ────────────────────────────────────────────────────────────────────

  @doc """
  Return the local ETS record count per partition plus a `:total` summary.

  This only reflects what is in the **local** ETS tables.  For a
  cluster-wide view use `cluster_stats/0`.

  ## Example

      Cache.stats()
      # => [
      #   {:"SuperCache.Storage.Ets_0", 1024},
      #   {:"SuperCache.Storage.Ets_1",  998},
      #   ...
      #   total: 8176
      # ]
  """
  @spec stats() :: keyword
  def stats() do
    entries =
      Partition.get_all_partition()
      |> List.flatten()
      |> Enum.map(&SuperCache.Storage.stats/1)

    total = Enum.reduce(entries, 0, fn {_, n}, acc -> acc + n end)
    entries ++ [total: total]
  end

  @doc """
  Return an aggregated cluster-wide statistics map.

  Gathers per-node record counts from every live node via `:erpc` and
  merges them with the cluster partition map from
  `SuperCache.Cluster.Stats.cluster/0`.

  The returned map contains all fields from `SuperCache.Cluster.Stats.cluster/0`
  plus:

  - `:node_stats` — `%{node => [partition_count: N, record_count: N]}`
  - `:unreachable_nodes` — nodes that did not respond within the timeout

  ## Example

      Cache.cluster_stats()
      # => %{
      #   nodes:             [:"a@host", :"b@host"],
      #   node_count:        2,
      #   replication_mode:  :async,
      #   num_partitions:    8,
      #   total_records:     1_042,
      #   node_stats:        %{:"a@host" => [partition_count: 4, record_count: 520],
      #                        :"b@host" => [partition_count: 4, record_count: 522]},
      #   unreachable_nodes: []
      # }
  """
  @spec cluster_stats() :: map
  def cluster_stats() do
    base = SuperCache.Cluster.Stats.cluster()
    live = Manager.live_nodes()

    {node_stats, unreachable} =
      live
      |> Task.async_stream(
        fn n ->
          try do
            local_stats = :erpc.call(n, __MODULE__, :stats, [], 5_000)
            total = Keyword.get(local_stats, :total, 0)

            part_count =
              local_stats
              |> Keyword.delete(:total)
              |> length()

            {n, [partition_count: part_count, record_count: total]}
          catch
            _, reason ->
              Logger.warning(
                "super_cache, distributed, cluster_stats failed on #{inspect(n)}: " <>
                  inspect(reason)
              )

              {n, :unreachable}
          end
        end,
        timeout: 8_000,
        on_timeout: :kill_task
      )
      |> Enum.reduce({%{}, []}, fn
        {:ok, {n, :unreachable}}, {stats, bad} -> {stats, [n | bad]}
        {:ok, {n, info}}, {stats, bad} -> {Map.put(stats, n, info), bad}
        {:exit, _}, {stats, bad} -> {stats, bad}
      end)

    base
    |> Map.put(:node_stats, node_stats)
    |> Map.put(:unreachable_nodes, unreachable)
  end

  ## ── 3PC helpers (called by Router via :erpc) ─────────────────────────────────

  @doc false
  # Apply a single op locally.  Called by the 3PC coordinator via :erpc — do
  # NOT call directly from application code.
  @spec apply_op_3pc(non_neg_integer, atom, any) :: :ok
  def apply_op_3pc(partition_idx, op_name, op_arg) do
    Replicator.apply_op(partition_idx, op_name, op_arg)
  end

  @doc false
  # Commit a batch of ops for a single partition using 3PC.  Invoked by
  # Router.route_put!/1 etc. when replication_mode == :strong.
  @spec commit_3pc(non_neg_integer, ThreePhaseCommit.op_list()) ::
          :ok | {:error, term}
  def commit_3pc(partition_idx, ops) do
    ThreePhaseCommit.commit(partition_idx, ops)
  end

  ## ── Private ──────────────────────────────────────────────────────────────────

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
