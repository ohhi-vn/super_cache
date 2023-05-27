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
    Logger.debug("stop storage workers (#{num})")

    for order <- 0..num-1 do
      prefix = Config.get_config(:table_prefix)
      name = String.to_atom("#{prefix}_#{order}")
      Logger.debug("remove storage #{inspect name}")
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
  Gets data by pattern matching in a partition.
  """
  @spec get_by_match(atom | tuple, atom | :ets.tid()) :: [list]
  def get_by_match(pattern, partition) do
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match(partition, pattern)
  end

  @doc """
  Gets data by match object in a partition.
  """
  @spec get_by_match_object(atom | tuple, atom | :ets.tid()) :: [tuple]
  def get_by_match_object(pattern, partition) do
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
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
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match_delete(partition, pattern)
  end
end
