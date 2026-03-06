defmodule SuperCache.Distributed.KeyValue do
  @moduledoc """
  Cluster-aware key-value store. Writes are routed to the primary node for
  the partition that owns the `kv_name` namespace; reads are served locally.

  API is identical to `SuperCache.KeyValue` — swap the alias to migrate.

  ## Partition strategy
  All keys in a namespace share the same partition, derived by hashing
  `kv_name` alone.  This means:
    - All reads and writes for a namespace hit the same ETS table.
    - One primary node owns all keys in a given namespace.
    - Cross-namespace operations never interfere.

  ## Example

      alias SuperCache.Distributed.KeyValue

      SuperCache.Cluster.Bootstrap.start!(...)
      KeyValue.add("session", :user_id, 42)
      KeyValue.get("session", :user_id)        # => 42
      KeyValue.remove("session", :user_id)
      KeyValue.remove_all("session")
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
  require Logger

  ## Public API ────────────────────────────────────────────────────────────────

  @doc "Store `value` under `key` in `kv_name`. Routed to primary."
  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, add key=#{inspect(key)}" end)
    route_write(kv_name, :local_put, [kv_name, key, value])
  end

  @doc "Retrieve value for `key` in `kv_name` from the local node."
  @spec get(any, any, any) :: any
  def get(kv_name, key, default \\ nil) do
    partition = Partition.get_partition(kv_name)
    case Storage.get({:kv, kv_name, key}, partition) do
      []           -> default
      [{_, value}] -> value
    end
  end

  @doc "Return all keys in `kv_name` from the local node."
  @spec keys(any) :: [any]
  def keys(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc "Return all values in `kv_name` from the local node."
  @spec values(any) :: [any]
  def values(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc "Return the number of entries in `kv_name` on the local node."
  @spec count(any) :: non_neg_integer
  def count(kv_name) do
    local_match(kv_name) |> length()
  end

  @doc "Remove `key` from `kv_name`. Routed to primary."
  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, remove key=#{inspect(key)}" end)
    route_write(kv_name, :local_delete, [kv_name, key])
  end

  @doc "Remove all entries in `kv_name`. Routed to primary."
  @spec remove_all(any) :: :ok
  def remove_all(kv_name) do
    route_write(kv_name, :local_delete_all, [kv_name])
  end

  @doc "Convert `kv_name` to `[{key, value}]` from the local node."
  @spec to_list(any) :: [{any, any}]
  def to_list(kv_name) do
    local_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ─────────────

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

  ## Private ───────────────────────────────────────────────────────────────────

  # All reads use kv_name as the partition key — same hash as writes.
  defp local_match(kv_name) do
    partition = Partition.get_partition(kv_name)
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, partition)
  end

  # Route a write to the primary for kv_name's partition.
  # Remote entry points are local_* functions — never route_* — to avoid
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
