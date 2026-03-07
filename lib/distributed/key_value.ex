
defmodule SuperCache.Distributed.KeyValue do
  @moduledoc """
  Cluster-aware key-value namespaces.

  Writes are routed to the primary node for the partition that owns the
  `kv_name` namespace; reads are served from the local node (eventual
  consistency).

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

  ## Example

      alias SuperCache.Distributed.KeyValue

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0, partition_pos: 0,
        cluster: :distributed, replication_factor: 2
      )

      KeyValue.add("feature_flags", :dark_mode,     true)
      KeyValue.add("feature_flags", :new_dashboard, false)

      KeyValue.get("feature_flags", :dark_mode)      # => true
      KeyValue.get("feature_flags", :absent, :off)   # => :off

      KeyValue.keys("feature_flags")    # => [:dark_mode, :new_dashboard]
      KeyValue.count("feature_flags")   # => 2

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
  Retrieve the value for `key` in `kv_name` from the **local node**.

  Returns `default` (default `nil`) when the key is absent.

  ## Example

      KeyValue.add("limits", :rate, 1_000)
      KeyValue.get("limits", :rate)           # => 1_000
      KeyValue.get("limits", :burst, 500)     # => 500 (default)
  """
  @spec get(any, any, any) :: any
  def get(kv_name, key, default \\ nil) do
    partition = Partition.get_partition(kv_name)
    case Storage.get({:kv, kv_name, key}, partition) do
      []           -> default
      [{_, value}] -> value
    end
  end

  @doc """
  Return all keys in `kv_name` from the local node.

  ## Example

      KeyValue.add("ns", :a, 1)
      KeyValue.add("ns", :b, 2)
      KeyValue.keys("ns") |> Enum.sort()   # => [:a, :b]
  """
  @spec keys(any) :: [any]
  def keys(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc """
  Return all values in `kv_name` from the local node.

  ## Example

      KeyValue.add("ns", :x, 10)
      KeyValue.add("ns", :y, 20)
      KeyValue.values("ns") |> Enum.sort()   # => [10, 20]
  """
  @spec values(any) :: [any]
  def values(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc """
  Return the number of entries in `kv_name` on the local node.

  ## Example

      KeyValue.add("ns", :a, 1)
      KeyValue.count("ns")   # => 1
  """
  @spec count(any) :: non_neg_integer
  def count(kv_name) do
    local_match(kv_name) |> length()
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

  @doc """
  Convert `kv_name` to `[{key, value}]` from the local node.

  Order is not guaranteed.

  ## Example

      KeyValue.add("ns", :p, 7)
      KeyValue.add("ns", :q, 8)
      KeyValue.to_list("ns") |> Enum.sort()   # => [{:p, 7}, {:q, 8}]
  """
  @spec to_list(any) :: [{any, any}]
  def to_list(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ──────────────

  @doc false
  def local_put(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    idx       = Partition.get_partition_order(kv_name)
    Storage.put({{:kv, kv_name, key}, value}, partition)
    Replicator.replicate(idx, :put, {{:kv, kv_name, key}, value})
    true
  end

  @doc false
  def local_delete(kv_name, key) do
    partition = Partition.get_partition(kv_name)
    idx       = Partition.get_partition_order(kv_name)
    Storage.delete({:kv, kv_name, key}, partition)
    Replicator.replicate(idx, :delete, {:kv, kv_name, key})
    :ok
  end

  @doc false
  def local_delete_all(kv_name) do
    partition = Partition.get_partition(kv_name)
    idx       = Partition.get_partition_order(kv_name)
    Storage.delete_match({{:kv, kv_name, :_}, :_}, partition)
    Replicator.replicate(idx, :delete_match, {{:kv, kv_name, :_}, :_})
    :ok
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp local_match(kv_name) do
    partition = Partition.get_partition(kv_name)
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, partition)
  end

  # Route a write to the primary for kv_name's partition.
  # Remote calls use local_* functions — never route_write — to prevent
  # forwarding loops.
  defp route_write(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
