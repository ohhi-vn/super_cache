defmodule SuperCache.Distributed.Struct do
  @moduledoc """
  Cluster-aware struct store.

  Writes (`add`, `remove`, `remove_all`) are routed to the primary node for
  the partition that owns the struct type. Reads (`get`, `get_all`) are
  served from the local node (eventual consistency).

  ## Partition strategy
  Every operation derives its partition by hashing `{:struct_storage, StructName}`.
  This is the namespace key — consistent across puts, deletes, and reads.

  API is identical to `SuperCache.Struct`.

  ## Example

      alias SuperCache.Distributed.Struct, as: DStruct

      SuperCache.Cluster.Bootstrap.start!(...)

      defmodule Person, do: defstruct [:id, :name, :age]

      DStruct.init(%Person{}, :id)
      DStruct.add(%Person{id: 1, name: "Alice", age: 30})
      DStruct.get(%Person{id: 1})      # => {:ok, %Person{...}}
      DStruct.remove(%Person{id: 1})
      DStruct.remove_all(%Person{})
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
  require Logger

  ## Public API ────────────────────────────────────────────────────────────────

  @doc "Register the key field for a struct type. Routed to primary."
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

  @doc "Store a struct. Routed to primary."
  @spec add(map) :: {:ok, map} | {:error, any}
  def add(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      route_write(struct, :local_add, [struct])
    end
  end

  @doc "Retrieve a struct by key. Read from local node."
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

  @doc "Return all structs of this type from the local node."
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

  @doc "Remove a struct by key. Routed to primary."
  @spec remove(map) :: {:ok, map} | {:error, any}
  def remove(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      # Read locally first to return the old value.
      case get(struct) do
        {:error, :not_found} = err -> err
        {:ok, existing}            -> route_write(struct, :local_remove, [struct])
                                      {:ok, existing}
      end
    end
  end

  @doc "Remove all structs of this type. Routed to primary."
  @spec remove_all(map) :: {:ok, :removed} | {:error, any}
  def remove_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      Logger.debug(fn -> "super_cache, dist.struct, remove_all #{inspect(struct_name)}" end)
      route_write(struct, :local_remove_all, [struct])
    end
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ─────────────

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

      # Overwrite — delete old then insert new.
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

  ## Private ───────────────────────────────────────────────────────────────────

  # Consistent namespace key used for ALL partition hashing in this module.
  # Every read, write, and delete hashes this same value.
  defp namespace(%{__struct__: struct_name}), do: {:struct_storage, struct_name}

  defp get_key_field(%{__struct__: struct_name} = struct) do
    ns        = namespace(struct)
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      []         -> {:error, :key_not_found}
      [{_, key}] -> {:ok, key}
    end
  end

  defp primary_for(struct) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _} = Manager.get_replicas(idx)
    primary
  end

  defp route_write(struct, fun, args) do
    primary = primary_for(struct)

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
