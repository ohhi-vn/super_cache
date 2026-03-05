defmodule SuperCache.Struct do
  @moduledoc """
  Store struct database in memory. Each struct storage is store in a partition.
  This is global cache any process can access.
  The struct storage is store in with cache.
  Need to start SuperCache.start!/1 before using this module.
  Ex:
  ```elixir
  alias SuperCache.Struct, as: Storage
  SuperCache.start!()

  defmodule Person do
    defstruct [
      :id,
      :name,
      :age
    ]
  end

  person = %Person{id: 1, name: "John", age: 30}

  # set key attribute of struct. default is :id field.
  Storage.init(%Person{}, :id)
  Storage.add(person)
  Storage.get(%Person{id: 1})
    # => "Hello"

  Storage.remove(%Person{id: 1})
  Storage.get(%Person{id: 1})
    # => nil

  person2 = %Person{id: 2, name: "Alice", age: 20}

  Storage.add(person2)

  # Remove all struct, keep key info.
  Storage.remove_all(%Person{})
  ```
  """

  alias SuperCache.Storage
  alias SuperCache.Partition

  require Logger

  @doc """
  Set key for struct. Need to run before using.
  """
  @spec init(map(), atom()) :: true
  def init(struct = %{__struct__: struct_name}, key \\ :id) when is_atom(key) do
    Logger.debug(
      "super_cache, struct, init storage for struct: #{inspect(struct_name)}, key attribute: #{inspect(key)}"
    )

    with true <- Map.has_key?(struct, key),
         {:error, :key_not_found} <-
           get_key_field(struct) do
      put_key_field(struct, key)
    else
      false -> {:error, "key is doesn't exist"}
      {:ok, _} -> {:error, "struct has initialized!"}
    end
  end

  @doc """
  Add a struct to cache. If has same key old struct will be removed.
  """
  @spec add(map()) :: {:ok, map()} | {:error, any()}
  def add(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)

    with {:ok, key} <- get_key_field(struct) do
      key_data = Map.get(struct, key)

      Logger.debug(
        "super_cache, struct, store struct: #{inspect(struct_name)}, key: #{inspect(key_data)} to partition: #{inspect(partition)}"
      )

      Storage.delete({{:struct_storage, :struct, struct_name}, key_data}, partition)

      Storage.put(
        {{{:struct_storage, :struct, struct_name}, key_data}, struct},
        partition
      )

      {:ok, struct}
    else
      failed ->
        Logger.warning(
          "super_cache, struct, failed to add new struct, reason: #{inspect(failed)}"
        )

        failed
    end
  end

  @doc """
  Get struct.
  Key is belong with struct.
  """
  @spec get(map()) :: {:ok, map()} | {:error, any()}
  def get(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)

    Logger.debug(
      "super_cache, struct, name: #{inspect(struct_name)} from partition: #{inspect(partition)}"
    )

    with {:ok, key} <- get_key_field(struct) do
      case Storage.get(
             {{:struct_storage, :struct, struct_name}, Map.get(struct, key)},
             partition
           ) do
        [] ->
          {:error, :not_found}

        [{_key, result}] ->
          {:ok, result}
      end
    else
      failed ->
        Logger.warning(
          "super_cache, struct, failed to add new struct, reason: #{inspect(failed)}"
        )

        failed
    end
  end

  @doc """
  Get all structs in cache.
  """
  @spec get_all(map()) :: {:ok, list()} | {:error, any()}
  def get_all(struct = %{__struct__: struct_name}) do
    with {:ok, _key} <- get_key_field(struct) do
      partition = get_partition(struct)

      result =
        Storage.get_by_match_object(
          {{{:struct_storage, :struct, struct_name}, :_}, :_},
          partition
        )
        |> Enum.map(fn {_, value} -> value end)

      {:ok, result}
    else
      failed ->
        Logger.warning(
          "super_cache, struct, failed to add new struct, reason: #{inspect(failed)}"
        )

        failed
    end
  end

  @doc """
  Remove struct has key in cache.
  """
  @spec remove(map) :: {:ok, struct} | {:error, any()}
  def remove(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)

    with {:ok, key} <- get_key_field(struct) do
      case Storage.get(
             {{:struct_storage, :struct, struct_name}, Map.get(struct, key)},
             partition
           ) do
        [] ->
          {:error, :not_found}

        [{_key, result}] ->
          key_data = Map.get(struct, key)

          Logger.debug(
            "super_cache, struct, remove #{inspect(struct_name)} with key: #{inspect(key_data)} from partition: #{inspect(partition)}"
          )

          Storage.delete(
            {{:struct_storage, :struct, struct_name}, key_data},
            partition
          )

          {:ok, result}
      end
    else
      failed ->
        Logger.warning(
          "super_cache, struct, failed to add new struct, reason: #{inspect(failed)}"
        )

        failed
    end
  end

  @doc """
  Remove all structs in cache.
  """
  @spec remove_all(map()) :: list()
  def remove_all(struct = %{__struct__: struct_name}) do
    with {:ok, _key} <- get_key_field(struct) do
      partition = get_partition(struct)

      Logger.debug(
        "super_cache, struct, remove all structs #{inspect(struct_name)} from partition: #{inspect(partition)}"
      )

      Storage.delete_match({{{:struct_storage, :struct, struct_name}, :_}, :_}, partition)
      {:ok, :removed}
    else
      failed ->
        Logger.warning(
          "super_cache, struct, failed to add new struct, reason: #{inspect(failed)}"
        )

        failed
    end
  end

  defp get_key_field(%{__struct__: struct_name} = struct) do
    partition = get_partition(struct)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      [] ->
        {:error, :key_not_found}

      [{_, key}] ->
        {:ok, key}
    end
  end

  defp put_key_field(struct = %{__struct__: struct_name}, key) do
    partition = get_partition(struct)

    Storage.put({{:struct_storage, :key, struct_name}, key}, partition)
  end

  defp get_partition(%{__struct__: struct_name}) do
    Partition.get_partition({:struct_storage, struct_name})
  end
end
