
 defmodule SuperCache.KeyValue do
  @moduledoc """
  Key-Value(KV) database in memory.
  Can use multiple KV cache by using different KV name.
  This is global KV cache any process can access.
  The KV is store in with cache.
  Need to start SuperCache.start!/1 before using this module.
  """

  alias SuperCache.Storage
  alias SuperCache.Partition

  require Logger

 @doc """
  Add key-value to cache. Key is used to get target partition to store data.
  Use can use multiple kv cache by using different kv_name.
  """
  @spec add(atom, any, any) :: true
  def add(kv_name, key, value) do
    part = Partition.get_partition(key)
    Logger.debug("super_cache, kv, name: #{inspect kv_name} store key: #{inspect key} to partition: #{inspect part}")
    # always delete old value before put new value.
    # TO-DO: Don't need to delete in set/ordered_set type.
    Storage.delete({:kv, kv_name, key}, part)
    Storage.put({{:kv, kv_name, key}, value}, part)
  end

  @doc """
  Get value by key.
  Key is belong wit kv_name.
  """
  @spec get(any, any, any) :: any
  def get(kv_name, key, default \\ nil) do
    part = Partition.get_partition(key)
    Logger.debug("super_cache, kv, name: #{inspect kv_name } get value of key: #{inspect key} from partition: #{inspect part}")
    case Storage.get({:kv, kv_name, key}, part) do
      [] -> default
      [{_, value}] -> value
    end
  end

  def remove(kv_name, key) do
    part = Partition.get_partition(key)
    Logger.debug("super_cache, kv, name: #{inspect kv_name } remove value of key: #{inspect key} from partition: #{inspect part}")
    Storage.delete({:kv, kv_name, key}, part)
  end

  @doc """
  Get all keys in kv_name cache.
  """
  @spec keys(any) :: [any]
  def keys(kv_name) do
    SuperCache.get_by_match_object!({{:kv, kv_name, :_}, :_})
    |>  Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc """
  Get all values in kv_name cache.
  """
  @spec values(any) :: [any]
  def values(kv_name) do
    SuperCache.get_by_match_object!({{:kv, kv_name, :_}, :_})
    |>  Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

end
