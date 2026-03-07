defmodule SuperCache.Struct do
  @moduledoc """
  In-memory struct store backed by SuperCache ETS partitions.

  Each struct type gets its own partition, determined by hashing the atom
  `{:struct_storage, StructName}`.  Structs are keyed by a single field
  that you declare with `init/2`.

  Any process in the VM can read or write any struct store by type.

  Requires `SuperCache.start!/1` to be called first.

  ## Setup

  Call `init/2` once per struct type before performing any other operations.
  It registers the key field in the partition.  Calling it again for the
  same type returns an error without modifying the existing registration.

  ## Example

      alias SuperCache.Struct, as: S

      SuperCache.start!()

      defmodule User do
        defstruct [:id, :name, :role]
      end

      # Register :id as the key field for User
      S.init(%User{}, :id)

      alice = %User{id: 1, name: "Alice", role: :admin}
      bob   = %User{id: 2, name: "Bob",   role: :viewer}

      S.add(alice)
      S.add(bob)

      S.get(%User{id: 1})      # => {:ok, %User{id: 1, name: "Alice", role: :admin}}
      S.get(%User{id: 99})     # => {:error, :not_found}

      {:ok, all} = S.get_all(%User{})
      length(all)              # => 2

      # Overwrite — insert a struct with the same key
      S.add(%User{id: 1, name: "Alice", role: :superadmin})
      S.get(%User{id: 1})      # => {:ok, %User{id: 1, name: "Alice", role: :superadmin}}

      S.remove(%User{id: 2})   # => {:ok, %User{id: 2, ...}}
      S.get(%User{id: 2})      # => {:error, :not_found}

      S.remove_all(%User{})    # => {:ok, :removed}

  ## Error returns

  All public functions return tagged tuples (`{:ok, …}` / `{:error, …}`)
  rather than raising, so the caller can pattern-match on the result.

  ## Cluster mode

  For distributed deployments use `SuperCache.Distributed.Struct`.  The API
  is identical; writes are routed to the primary node for the struct's
  partition.
  """

  alias SuperCache.{Storage, Partition}
  require Logger

  @doc """
  Register `key` as the lookup field for the struct type of `struct`.

  Must be called once before `add/1`, `get/1`, etc.

  Returns `true` on success, or an error tuple if:
  - `key` does not exist on the struct (`{:error, "key is doesn't exist"}`).
  - The struct type was already initialised (`{:error, "struct has initialized!"}`).

  ## Example

      defmodule Product do
        defstruct [:sku, :name, :price]
      end

      SuperCache.Struct.init(%Product{}, :sku)   # => true
      SuperCache.Struct.init(%Product{}, :sku)   # => {:error, "struct has initialized!"}
      SuperCache.Struct.init(%Product{}, :nope)  # => {:error, "key is doesn't exist"}
  """
  @spec init(map(), atom()) :: true | {:error, any()}
  def init(struct = %{__struct__: struct_name}, key \\ :id) when is_atom(key) do
    Logger.debug(fn ->
      "super_cache, struct, init #{inspect(struct_name)} key=#{inspect(key)}"
    end)

    with true <- Map.has_key?(struct, key),
         {:error, :key_not_found} <- get_key_field(struct) do
      put_key_field(struct, key)
    else
      false    -> {:error, "key is doesn't exist"}
      {:ok, _} -> {:error, "struct has initialized!"}
    end
  end

  @doc """
  Store `struct` in the cache.

  If a struct with the same key already exists it is **overwritten**.

  Returns `{:ok, struct}` on success, or `{:error, reason}` if `init/2` was
  never called for this struct type.

  ## Example

      S.add(%User{id: 1, name: "Alice", role: :admin})
      # => {:ok, %User{id: 1, name: "Alice", role: :admin}}
  """
  @spec add(map()) :: {:ok, map()} | {:error, any()}
  def add(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)

    with {:ok, key} <- get_key_field(struct) do
      key_data = Map.get(struct, key)
      Logger.debug(fn ->
        "super_cache, struct, add #{inspect(struct_name)} key=#{inspect(key_data)}"
      end)
      Storage.delete({{:struct_storage, :struct, struct_name}, key_data}, partition)
      Storage.put({{{:struct_storage, :struct, struct_name}, key_data}, struct}, partition)
      {:ok, struct}
    else
      failed ->
        Logger.warning("super_cache, struct, add failed: #{inspect(failed)}")
        failed
    end
  end

  @doc """
  Retrieve the struct whose key field equals the corresponding field in `struct`.

  Only the key field of `struct` is used for lookup; all other fields are
  ignored.

  Returns `{:ok, stored_struct}` or `{:error, :not_found}`.

  ## Example

      S.get(%User{id: 1})    # => {:ok, %User{id: 1, name: "Alice", role: :admin}}
      S.get(%User{id: 99})   # => {:error, :not_found}
  """
  @spec get(map()) :: {:ok, map()} | {:error, any()}
  def get(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)
    Logger.debug("super_cache, struct, get #{inspect(struct_name)}")

    with {:ok, key} <- get_key_field(struct) do
      case Storage.get(
             {{:struct_storage, :struct, struct_name}, Map.get(struct, key)},
             partition
           ) do
        []          -> {:error, :not_found}
        [{_, result}] -> {:ok, result}
      end
    else
      failed ->
        Logger.warning("super_cache, struct, get failed: #{inspect(failed)}")
        failed
    end
  end

  @doc """
  Return all stored structs of the same type as `struct`.

  Returns `{:ok, [struct]}`.  The list is empty when nothing has been stored.

  ## Example

      S.add(%User{id: 1, name: "Alice", role: :admin})
      S.add(%User{id: 2, name: "Bob",   role: :viewer})

      {:ok, users} = S.get_all(%User{})
      length(users)   # => 2
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
        Logger.warning("super_cache, struct, get_all failed: #{inspect(failed)}")
        failed
    end
  end

  @doc """
  Remove the struct whose key matches the key field of `struct`.

  Returns `{:ok, removed_struct}` or `{:error, :not_found}`.

  ## Example

      S.remove(%User{id: 1})   # => {:ok, %User{id: 1, ...}}
      S.remove(%User{id: 1})   # => {:error, :not_found}
  """
  @spec remove(map) :: {:ok, map} | {:error, any()}
  def remove(struct = %{__struct__: struct_name}) do
    partition = get_partition(struct)

    with {:ok, key} <- get_key_field(struct) do
      key_data = Map.get(struct, key)

      case Storage.get({{:struct_storage, :struct, struct_name}, key_data}, partition) do
        [] ->
          {:error, :not_found}

        [{_, result}] ->
          Logger.debug(fn ->
            "super_cache, struct, remove #{inspect(struct_name)} key=#{inspect(key_data)}"
          end)
          Storage.delete({{:struct_storage, :struct, struct_name}, key_data}, partition)
          {:ok, result}
      end
    else
      failed ->
        Logger.warning("super_cache, struct, remove failed: #{inspect(failed)}")
        failed
    end
  end

  @doc """
  Remove all stored structs of the same type as `struct`.

  The key field registration created by `init/2` is **preserved**, so you
  can continue using `add/1` after calling `remove_all/1`.

  Returns `{:ok, :removed}`.

  ## Example

      S.add(%User{id: 1, name: "Alice", role: :admin})
      S.add(%User{id: 2, name: "Bob",   role: :viewer})
      S.remove_all(%User{})          # => {:ok, :removed}
      S.get_all(%User{})             # => {:ok, []}
  """
  @spec remove_all(map()) :: {:ok, :removed} | {:error, any()}
  def remove_all(struct = %{__struct__: struct_name}) do
    with {:ok, _key} <- get_key_field(struct) do
      partition = get_partition(struct)
      Logger.debug(fn -> "super_cache, struct, remove_all #{inspect(struct_name)}" end)
      Storage.delete_match({{{:struct_storage, :struct, struct_name}, :_}, :_}, partition)
      {:ok, :removed}
    else
      failed ->
        Logger.warning("super_cache, struct, remove_all failed: #{inspect(failed)}")
        failed
    end
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp get_key_field(%{__struct__: struct_name} = struct) do
    partition = get_partition(struct)
    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      []         -> {:error, :key_not_found}
      [{_, key}] -> {:ok, key}
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
