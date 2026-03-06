defmodule SuperCache.KeyValue do
  @moduledoc """
  In-memory key-value store backed by SuperCache ETS partitions.

  Multiple independent KV namespaces can coexist by using different `kv_name`
  values.  Data is globally accessible from any process in the VM.

  Requires `SuperCache.start!/1` to be called first.

  ## Example

      alias SuperCache.KeyValue

      SuperCache.start!()

      KeyValue.add("session", :user_id, 42)
      KeyValue.get("session", :user_id)       # => 42
      KeyValue.keys("session")                # => [:user_id]

      KeyValue.remove("session", :user_id)
      KeyValue.get("session", :user_id)       # => nil

      KeyValue.add("session", :token, "abc")
      KeyValue.remove_all("session")
      KeyValue.get("session", :token)         # => nil
  """

  alias SuperCache.{Storage, Partition}

  require Logger

  @doc """
  Store `value` under `key` in the `kv_name` namespace.

  Overwrites any existing value for the same key.
  All three arguments can be any Elixir term.
  """
  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    part = Partition.get_partition(kv_name)

    Logger.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, add key=#{inspect(key)} partition=#{inspect(part)}"
    end)

    # For :set / :ordered_set tables, Ets.insert/2 overwrites automatically —
    # no explicit delete needed.
    Storage.put({{:kv, kv_name, key}, value}, part)
  end

  @doc """
  Retrieve the value for `key` in `kv_name`.
  Returns `default` (default `nil`) when the key does not exist.
  """
  @spec get(any, any, any) :: any
  def get(kv_name, key, default \\ nil) do
    part = Partition.get_partition(kv_name)

    Logger.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, get key=#{inspect(key)} partition=#{inspect(part)}"
    end)

    case Storage.get({:kv, kv_name, key}, part) do
      [] -> default
      [{_, value}] -> value
    end
  end

  @doc "Return all keys in the `kv_name` namespace."
  @spec keys(any) :: [any]
  def keys(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |> Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc "Return all values in the `kv_name` namespace."
  @spec values(any) :: [any]
  def values(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |> Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc "Return the number of entries in the `kv_name` namespace."
  @spec count(any) :: non_neg_integer
  def count(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |> length()
  end

  @doc "Remove the entry for `key` from `kv_name`.  Always succeeds."
  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    part = Partition.get_partition(kv_name)

    Logger.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, remove key=#{inspect(key)} partition=#{inspect(part)}"
    end)

    Storage.delete({:kv, kv_name, key}, part)
    :ok
  end

  @doc "Remove all entries in the `kv_name` namespace."
  @spec remove_all(any) :: :ok
  def remove_all(kv_name) do
    SuperCache.delete_by_match!(kv_name, {{:kv, kv_name, :_}, :_})
  end

  @doc """
  Convert the `kv_name` namespace to a `[{key, value}]` list.
  Order is not guaranteed.
  """
  @spec to_list(any) :: [{any, any}]
  def to_list(kv_name) do
    SuperCache.get_by_match_object!(kv_name, {{:kv, kv_name, :_}, :_})
    |> Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end
end
