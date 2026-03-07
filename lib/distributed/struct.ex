

defmodule SuperCache.Distributed.Struct do
  @moduledoc """
  Cluster-aware struct store.

  Writes (`add`, `remove`, `remove_all`, `init`) are routed to the primary
  node for the partition that owns the struct type.  Reads (`get`, `get_all`)
  are served from the local node (eventual consistency).

  The API is identical to `SuperCache.Struct` — swap the alias to migrate:

      # Single-node
      alias SuperCache.Struct, as: S

      # Cluster-aware
      alias SuperCache.Distributed.Struct, as: S

  ## Partition strategy

  Every operation hashes `{:struct_storage, StructName}` to find the
  partition.  This namespace key is consistent across puts, deletes, and
  reads, so all operations for a struct type always hit the same ETS table
  regardless of which node handles the request.

  ## Example

      alias SuperCache.Distributed.Struct, as: DS

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0, partition_pos: 0,
        cluster: :distributed, replication_factor: 2
      )

      defmodule Order do
        defstruct [:id, :customer, :total, :status]
      end

      DS.init(%Order{}, :id)

      DS.add(%Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending})
      DS.add(%Order{id: "o-2", customer: "Bob",   total: 12.00, status: :shipped})

      DS.get(%Order{id: "o-1"})
      # => {:ok, %Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending}}

      {:ok, all} = DS.get_all(%Order{})
      length(all)   # => 2

      DS.remove(%Order{id: "o-1"})
      DS.remove_all(%Order{})
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
  require Logger

  ## Public API ─────────────────────────────────────────────────────────────────

  @doc """
  Register `key` as the lookup field for the struct type of `struct`.

  Routed to the primary node.  Must be called before any other operation on
  this struct type.

  ## Example

      DS.init(%Order{}, :id)    # => true
      DS.init(%Order{}, :id)    # => {:error, "struct already initialised"}
  """
  @spec init(map, atom) :: true | {:error, any}
  def init(%{__struct__: _} = struct, key \\ :id) when is_atom(key) do
    with true <- Map.has_key?(struct, key),
         {:error, :key_not_found} <- get_key_field(struct) do
      route_write(struct, :local_init, [struct, key])
    else
      false    -> {:error, "key does not exist on struct"}
      {:ok, _} -> {:error, "struct already initialised"}
    end
  end

  @doc """
  Store `struct`.  Routed to the primary node.

  Overwrites any existing struct with the same key.

  ## Example

      DS.add(%Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending})
      # => {:ok, %Order{...}}
  """
  @spec add(map) :: {:ok, map} | {:error, any}
  def add(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      route_write(struct, :local_add, [struct])
    end
  end

  @doc """
  Retrieve a struct by its key field from the **local node**.

  Returns `{:ok, struct}` or `{:error, :not_found}`.

  ## Example

      DS.get(%Order{id: "o-1"})    # => {:ok, %Order{...}}
      DS.get(%Order{id: "nope"})   # => {:error, :not_found}
  """
  @spec get(map) :: {:ok, map} | {:error, :not_found | any}
  def get(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns        = namespace(struct)
      partition = Partition.get_partition(ns)
      ets_key   = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}

      case Storage.get(ets_key, partition) do
        []            -> {:error, :not_found}
        [{_, result}] -> {:ok, result}
      end
    end
  end

  @doc """
  Return all structs of this type from the **local node**.

  Returns `{:ok, [struct]}`.

  ## Example

      {:ok, orders} = DS.get_all(%Order{})
      Enum.map(orders, & &1.status)
  """
  @spec get_all(map) :: {:ok, list} | {:error, any}
  def get_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      ns        = namespace(struct)
      partition = Partition.get_partition(ns)

      results =
        Storage.get_by_match_object(
          {{{:struct_storage, :struct, struct_name}, :_}, :_},
          partition
        )
        |> Enum.map(fn {_, value} -> value end)

      {:ok, results}
    end
  end

  @doc """
  Remove the struct matching the key field of `struct`.  Routed to the primary.

  Returns `{:ok, removed_struct}` or `{:error, :not_found}`.

  ## Example

      DS.remove(%Order{id: "o-1"})   # => {:ok, %Order{...}}
      DS.remove(%Order{id: "o-1"})   # => {:error, :not_found}
  """
  @spec remove(map) :: {:ok, map} | {:error, any}
  def remove(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      case get(struct) do
        {:error, :not_found} = err -> err
        {:ok, _existing}  = return          ->
          case route_write(struct, :local_remove, [struct]) do
           :ok -> return
           {:ok, _} -> return
           other -> other
          end
      end
    end
  end

  @doc """
  Remove all structs of this type.  Routed to the primary.

  The key field registration is preserved; you can call `add/1` again
  without calling `init/2`.

  ## Example

      DS.remove_all(%Order{})    # => {:ok, :removed}
      DS.get_all(%Order{})       # => {:ok, []}
  """
  @spec remove_all(map) :: {:ok, :removed} | {:error, any}
  def remove_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      Logger.debug(fn -> "super_cache, dist.struct, remove_all #{inspect(struct_name)}" end)
      case route_write(struct, :local_remove_all, [struct]) do
        :ok -> {:ok, :removed}
        {:ok, _} -> {:ok, :removed}
        other -> other
      end
    end
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ──────────────

  @doc false
  def local_init(%{__struct__: struct_name} = struct, key) do
    ns        = namespace(struct)
    partition = Partition.get_partition(ns)
    idx       = Partition.get_partition_order(ns)
    record    = {{:struct_storage, :key, struct_name}, key}
    Storage.put(record, partition)
    Replicator.replicate(idx, :put, record)
    true
  end

  @doc false
  def local_add(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns        = namespace(struct)
      partition = Partition.get_partition(ns)
      idx       = Partition.get_partition_order(ns)
      key_data  = Map.get(struct, key)
      ets_key   = {{:struct_storage, :struct, struct_name}, key_data}

      Logger.debug(fn ->
        "super_cache, dist.struct, add #{inspect(struct_name)} key=#{inspect(key_data)}"
      end)

      Storage.delete(ets_key, partition)
      Storage.put({ets_key, struct}, partition)
      Replicator.replicate(idx, :delete, ets_key)
      Replicator.replicate(idx, :put, {ets_key, struct})
      {:ok, struct}
    end
  end

  @doc false
  def local_remove(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns        = namespace(struct)
      partition = Partition.get_partition(ns)
      idx       = Partition.get_partition_order(ns)
      ets_key   = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}
      Storage.delete(ets_key, partition)
      Replicator.replicate(idx, :delete, ets_key)
      :ok
    end
  end

  @doc false
  def local_remove_all(%{__struct__: struct_name} = struct) do
    ns        = namespace(struct)
    partition = Partition.get_partition(ns)
    idx       = Partition.get_partition_order(ns)
    pattern   = {{{:struct_storage, :struct, struct_name}, :_}, :_}
    Storage.delete_match(pattern, partition)
    Replicator.replicate(idx, :delete_match, pattern)
    {:ok, :removed}
  end

  ## Private ────────────────────────────────────────────────────────────────────

  # Consistent namespace key for ALL partition hashing in this module.
  defp namespace(%{__struct__: struct_name}), do: {:struct_storage, struct_name}

  defp get_key_field(%{__struct__: struct_name} = struct) do
    ns        = namespace(struct)
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      []         -> {:error, :key_not_found}
      [{_, key}] -> {:ok, key}
    end
  end

  defp route_write(struct, fun, args) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.struct, fwd #{fun} → #{inspect(primary)}"
      end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
