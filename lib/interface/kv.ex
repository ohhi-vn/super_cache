
 defmodule SuperCache.KeyValue do
  @moduledoc """
  Key-Value(KV) database in memory. Each KV name is store in a partition.
  Can use multiple KV cache by using different KV name.
  This is global KV cache any process can access.
  The KV is store in with cache.
  Need to start SuperCache.start!/1 before using this module.
  Ex:
  ```
  alias SuperCache.KeyValue
  SuperCache.start!()
  KeyValue.add("my_kv", :key, "Hello")
  KeyValue.get("my_kv", :key)
    # => "Hello"

  KeyValue.remove("my_kv", :key)
  KeyValue.get("my_kv", :key)
    # => nil

  KeyValue.add("my_kv", :key, "Hello")
  KeyValue.remove_all("my_kv")
  ```
  """

  alias SuperCache.Storage
  alias SuperCache.Partition

  require Logger

 @doc """
  Add key-value to cache. kv_name is used to get target partition to store data.
  Use can use multiple kv cache by using different kv_name.
  You can use any type of kv_name, key & value.
  Format of data is {{:kv, kv_name, key}, value} in ets table.
  You can use other functions of SuperCache to get data.
  """
  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    part = Partition.get_partition(kv_name)
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
    part = Partition.get_partition(kv_name)
    Logger.debug("super_cache, kv, name: #{inspect kv_name } get value of key: #{inspect key} from partition: #{inspect part}")
    case Storage.get({:kv, kv_name, key}, part) do
      [] -> default
      [{_, value}] -> value
    end
  end

  @doc """
  Get all keys in kv_name cache.
  """
  @spec keys(any) :: [any]
  def keys(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |>  Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc """
  Get all values in kv_name cache.
  """
  @spec values(any) :: [any]
  def values(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |>  Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc """
  Count number of key-value in kv_name cache.
  """
  @spec count(any) :: integer
  def count(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |> Enum.count()
  end


  @doc """
  Remove key-value in kv_name cache. This operaton is always success.
  """
  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    part = Partition.get_partition(kv_name)
    Logger.debug("super_cache, kv, name: #{inspect kv_name } remove value of key: #{inspect key} from partition: #{inspect part}")
    Storage.delete({:kv, kv_name, key}, part)
    :ok
  end

  @doc """
  Remove all key-value in kv_name cache.
  """
  @spec remove_all(any()) :: list()
  def remove_all(kv_name) do
    SuperCache.delete_by_match!(kv_name, {{:kv, kv_name, :_}, :_})
  end

  @doc """
  Convert kv_name cache to list of key-value.
  result has format [{key, value}]
  """
  @spec to_list(any) :: [{any, any}]
  def to_list(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |>  Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end
end
