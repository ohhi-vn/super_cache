defmodule SuperCache.Storage do
  @moduledoc false

  require Logger

  alias  SuperCache.{EtsHolder, Config}
  alias :ets, as: Ets

  @doc """
  Create all Ets tables.
  """
  @spec start(pos_integer) :: any
  def start(num) when is_integer(num) and (num > 0) do
      for order <- 0..num-1 do
        prefix = Config.get_config(:table_prefix)
        name = String.to_atom("#{prefix}_#{order}")
        EtsHolder.new_table(EtsHolder, name)
      end
  end

  @doc """
  Delete all Ets tables.
  """
  @spec stop(pos_integer) :: any
  def stop(num) when is_integer(num) and (num > 0) do
    Logger.debug("super_cache, storage, stop storage workers (#{num})")

    for order <- 0..num-1 do
      prefix = Config.get_config(:table_prefix)
      name = String.to_atom("#{prefix}_#{order}")
      Logger.debug("super_cache, storage, remove storage #{inspect name}")
      EtsHolder.delete_table(EtsHolder, name)
    end
  end

  @doc """
  Puts data(tuple) to a GenServer which has id is partition.
  """
  @spec put([tuple] | tuple, atom | :ets.tid()) :: true
  def put(term, partition) do
    Ets.insert(partition, term)
  end

  @doc """
  Gets data from GenServer (identity by partition) with key.
  """
  @spec get(any, atom | :ets.tid()) :: [tuple]
  def get(key, partition) do
    Ets.lookup(partition, key)
  end

  @doc """
  Update data in a partition.
  element_spec is a tuple with 2 elements {pos_integer, any} or list of 2 element tuple.
  If key is not existed, default value will be used.
  Update element just works with set/ordered_set type.
  """
  def update_element(key, partition, element_spec, default) do
    Ets.update_element(partition, key, element_spec, default)
  end

  @doc """
  Update data in a partition.
  element_spec is a tuple with 2 elements {pos_integer, any} or list of 2 element tuple.
  Update element just works with set/ordered_set type.
  """
  def update_element(key, partition, element_spec) do
    Ets.update_element(partition, key, element_spec)
  end

  @doc """
  Update counter in a partition.
  counter_spec is a tuple {pos, increment} or {pos, increment, threshold, setvalue}.
  Update counter just works with set/ordered_set type.
  """
  def update_counter(key, partition, counter_spec) do
    Ets.update_counter(partition, key, counter_spec)
  end

  @doc """
  Update counter in a partition.
  counter_spec is a tuple {pos, increment} or {pos, increment, threshold, setvalue}.
  If key is not existed, default value will be used.
  Update counter just works with set/ordered_set type.
  """
  def update_counter(key, partition, counter_spec, default) do
    Ets.update_counter(partition, key, counter_spec, default)
  end

  @doc """
  Gets data by pattern matching in a partition.
  """
  @spec get_by_match(atom | tuple, atom | :ets.tid()) :: [list]
  def get_by_match(pattern, partition) do
    Logger.debug("super_cache, storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match(partition, pattern)
  end

  @doc """
  Gets data by match object in a partition.
  """
  @spec get_by_match_object(atom | tuple, atom | :ets.tid()) :: [tuple]
  def get_by_match_object(pattern, partition) do
    Logger.debug("super_cache, storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match_object(partition, pattern)
  end

  @doc """
  Scans all object in a partition.
  """
  @spec scan((any, any -> any), any, atom | :ets.tid()) :: any
  def scan(fun, acc, partition) do
    Ets.foldl(fun, acc, partition)
  end

  @doc """
  Deletes data in a partition.
  """
  @spec delete(any, atom | :ets.tid()) :: true
  def delete(key, partition) do
    Ets.delete(partition, key)
  end

  @doc """
  Deletes all data in a partition.
  """
  @spec delete_all(atom | :ets.tid()) :: true
  def delete_all(partition) do
    Ets.delete_all_objects(partition)
  end

  @doc """
  Deletes by pattern in a partition.
  """
  @spec delete_match(atom | tuple, atom | :ets.tid()) :: true
  def delete_match(pattern, partition) do
    Logger.debug("super_cache, storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match_delete(partition, pattern)
  end

  @doc """
  Takes data in a partition.
  Data is removed after taking.
  """
  @spec take(any, atom | :ets.tid()) :: [tuple]
  def take(key, partition) do
    Ets.take(partition, key)
  end

  @doc """
  Get size in partition.
  """
  def stats(partition) do
    counter = Ets.info(partition, :size)
    {partition, counter}
  end
end
