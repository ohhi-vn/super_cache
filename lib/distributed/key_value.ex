defmodule SuperCache.Distributed.KeyValue do
  @moduledoc """
  Cluster-aware key-value namespaces.

  Writes are routed to the primary node for the partition that owns the
  `kv_name` namespace.  Reads default to the local node (eventual
  consistency) but can be directed to the primary or resolved via quorum
  when stronger consistency is required.

  The API is identical to `SuperCache.KeyValue` — swap the alias to migrate:

      # Single-node
      alias SuperCache.KeyValue

      # Cluster-aware
      alias SuperCache.Distributed.KeyValue

  ## Partition strategy

  All keys in a namespace share one partition, derived by hashing `kv_name`
  alone.  This means:

  - All writes for a namespace are serialised through one primary node.
  - Reads from any replica return local ETS directly, with no network hop.
  - Namespaces with different names never interfere, even if they hash to the
    same partition.

  ## Read modes

  | Mode       | Consistency             | Latency               |
  |------------|-------------------------|-----------------------|
  | `:local`   | Eventual (default)      | Zero extra latency    |
  | `:primary` | Strong (per key)        | +1 RTT if non-primary |
  | `:quorum`  | Majority vote           | +1 RTT (parallel)     |

  Use `:primary` or `:quorum` when this node may not hold the partition
  at all, or when you need to read your own writes from any node.

  ## Example

      alias SuperCache.Distributed.KeyValue

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0, partition_pos: 0,
        cluster: :distributed, replication_factor: 2
      )

      KeyValue.add("feature_flags", :dark_mode,     true)
      KeyValue.add("feature_flags", :new_dashboard, false)

      KeyValue.get("feature_flags", :dark_mode)                            # => true
      KeyValue.get("feature_flags", :absent, :off)                         # => :off
      KeyValue.get("feature_flags", :dark_mode, nil, read_mode: :primary)  # => true

      KeyValue.keys("feature_flags")                           # => [:dark_mode, :new_dashboard]
      KeyValue.keys("feature_flags", read_mode: :primary)      # => [:dark_mode, :new_dashboard]

      KeyValue.count("feature_flags")                          # => 2
      KeyValue.remove("feature_flags", :dark_mode)
      KeyValue.remove_all("feature_flags")
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
  require Logger

  ## Public API ─────────────────────────────────────────────────────────────────

  @doc """
  Store `value` under `key` in `kv_name`.  Routed to the primary node.

  ## Example

      KeyValue.add("limits", :rate, 1_000)
  """
  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, add key=#{inspect(key)}" end)
    route_write(kv_name, :local_put, [kv_name, key, value])
  end

  @doc """
  Retrieve the value for `key` in `kv_name`.

  Returns `default` (default `nil`) when the key is absent.  Defaults to
  reading from the **local node**; pass `read_mode: :primary` or
  `read_mode: :quorum` when the local replica may be stale or absent.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      KeyValue.get("limits", :rate)
      KeyValue.get("limits", :burst, 500)
      KeyValue.get("limits", :rate, nil, read_mode: :primary)
  """
  @spec get(any, any, any, keyword) :: any
  def get(kv_name, key, default \\ nil, opts \\ []) do
    route_read(kv_name, :local_get, [kv_name, key, default], opts)
  end

  @doc """
  Return all keys in `kv_name`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      KeyValue.keys("ns") |> Enum.sort()
      KeyValue.keys("ns", read_mode: :primary) |> Enum.sort()
  """
  @spec keys(any, keyword) :: [any]
  def keys(kv_name, opts \\ []) do
    route_read(kv_name, :local_keys, [kv_name], opts)
  end

  @doc """
  Return all values in `kv_name`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      KeyValue.values("ns") |> Enum.sort()
      KeyValue.values("ns", read_mode: :quorum) |> Enum.sort()
  """
  @spec values(any, keyword) :: [any]
  def values(kv_name, opts \\ []) do
    route_read(kv_name, :local_values, [kv_name], opts)
  end

  @doc """
  Return the number of entries in `kv_name`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      KeyValue.count("ns")
      KeyValue.count("ns", read_mode: :primary)
  """
  @spec count(any, keyword) :: non_neg_integer
  def count(kv_name, opts \\ []) do
    route_read(kv_name, :local_count, [kv_name], opts)
  end

  @doc """
  Convert `kv_name` to `[{key, value}]`.  Order is not guaranteed.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      KeyValue.to_list("ns") |> Enum.sort()
      KeyValue.to_list("ns", read_mode: :primary) |> Enum.sort()
  """
  @spec to_list(any, keyword) :: [{any, any}]
  def to_list(kv_name, opts \\ []) do
    route_read(kv_name, :local_to_list, [kv_name], opts)
  end

  @doc """
  Remove `key` from `kv_name`.  Routed to the primary node.

  ## Example

      KeyValue.remove("ns", :stale_key)
  """
  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, remove key=#{inspect(key)}" end)
    route_write(kv_name, :local_delete, [kv_name, key])
  end

  @doc """
  Remove all entries in `kv_name`.  Routed to the primary node.

  ## Example

      KeyValue.remove_all("feature_flags")
      KeyValue.count("feature_flags")   # => 0
  """
  @spec remove_all(any) :: :ok
  def remove_all(kv_name) do
    route_write(kv_name, :local_delete_all, [kv_name])
  end

  ## Remote entry points — writes (called via :erpc — do NOT call directly) ────

  @doc false
  def local_put(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    Storage.put({{:kv, kv_name, key}, value}, partition)
    Replicator.replicate(idx, :put, {{:kv, kv_name, key}, value})
    true
  end

  @doc false
  def local_delete(kv_name, key) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    Storage.delete({:kv, kv_name, key}, partition)
    Replicator.replicate(idx, :delete, {:kv, kv_name, key})
    :ok
  end

  @doc false
  def local_delete_all(kv_name) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    Storage.delete_match({{:kv, kv_name, :_}, :_}, partition)
    Replicator.replicate(idx, :delete_match, {{:kv, kv_name, :_}, :_})
    :ok
  end

  ## Remote entry points — reads (called via :erpc — do NOT call directly) ─────

  @doc false
  def local_get(kv_name, key, default) do
    partition = Partition.get_partition(kv_name)

    case Storage.get({:kv, kv_name, key}, partition) do
      [] -> default
      [{_, value}] -> value
    end
  end

  @doc false
  def local_keys(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc false
  def local_values(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc false
  def local_count(kv_name) do
    do_match(kv_name) |> length()
  end

  @doc false
  def local_to_list(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp do_match(kv_name) do
    partition = Partition.get_partition(kv_name)
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, partition)
  end

  # ── Write routing ──────────────────────────────────────────────────────────

  defp route_write(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.kv #{inspect(kv_name)}, fwd #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # ── Read routing ───────────────────────────────────────────────────────────

  defp route_read(kv_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)

    # If this node owns neither the primary nor a replica for this partition,
    # a local ETS read will always return empty — fall back to :primary
    # regardless of the requested mode (quorum still fans out correctly since
    # it includes the primary in the node list).
    effective_mode =
      if mode == :local and not has_partition?(kv_name), do: :primary, else: mode

    case effective_mode do
      :local -> apply(__MODULE__, fun, args)
      :primary -> route_read_primary(kv_name, fun, args)
      :quorum -> route_read_quorum(kv_name, fun, args)
    end
  end

  # Returns true when this node holds the primary or at least one replica.
  defp has_partition?(kv_name) do
    idx = Partition.get_partition_order(kv_name)
    {primary, replicas} = Manager.get_replicas(idx)
    node() in [primary | replicas]
  end

  # :primary — forward to the primary if this node is not the primary.
  defp route_read_primary(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.kv #{inspect(kv_name)}, read_primary #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # :quorum — ask every replica in parallel; return the majority result.
  # Falls back to the primary's answer on a tie.
  defp route_read_quorum(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, replicas} = Manager.get_replicas(idx)
    all_nodes = [primary | replicas]

    results =
      all_nodes
      |> Task.async_stream(
        fn
          n when n == node() -> apply(__MODULE__, fun, args)
          n -> :erpc.call(n, __MODULE__, fun, args, 5_000)
        end,
        timeout: 5_000,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, result} -> [result]
        # timed-out or crashed node → skip
        _ -> []
      end)

    quorum_result(results, fn ->
      if primary == node() do
        apply(__MODULE__, fun, args)
      else
        :erpc.call(primary, __MODULE__, fun, args, 5_000)
      end
    end)
  end

  # Choose the result appearing in a strict majority; tie-breaks via primary.
  defp quorum_result(results, tiebreak_fn) do
    majority = div(length(results), 2) + 1

    case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
      {result, _} -> result
      nil -> tiebreak_fn.()
    end
  end
end
