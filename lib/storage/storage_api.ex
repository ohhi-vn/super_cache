defmodule SuperCache.Storage do
  @moduledoc false

  require Logger

  alias SuperCache.{EtsHolder, Config}
  alias :ets, as: Ets

  ## Lifecycle ##

  @doc "Create all ETS partitions."
  @spec start(pos_integer) :: :ok
  def start(num) when is_integer(num) and num > 0 do
    prefix = Config.get_config(:table_prefix)

    for order <- 0..(num - 1) do
      name = table_name(prefix, order)
      EtsHolder.new_table(EtsHolder, name)
    end

    :ok
  end

  @doc "Delete all ETS partitions."
  @spec stop(pos_integer) :: :ok
  def stop(num) when is_integer(num) and num > 0 do
    Logger.debug(fn -> "super_cache, storage, stopping #{num} partition(s)" end)
    prefix = Config.get_config(:table_prefix)

    for order <- 0..(num - 1) do
      name = table_name(prefix, order)
      Logger.debug(fn -> "super_cache, storage, deleting #{inspect(name)}" end)
      EtsHolder.delete_table(EtsHolder, name)
    end

    :ok
  end

  ## Write ##

  @doc "Insert one or more tuples into a partition."
  @spec put([tuple] | tuple, atom | :ets.tid()) :: true
  def put(term, partition), do: Ets.insert(partition, term)

  @doc """
  Insert a tuple only if no entry with the same key exists.
  Returns `true` on success, `false` if the key is already present.
  Works only with `:set` / `:ordered_set` table types.
  """
  @spec insert_new(tuple, atom | :ets.tid()) :: boolean
  def insert_new(term, partition), do: Ets.insert_new(partition, term)

  @doc "Update specific element(s) of an existing record.  Optional default for missing keys."
  def update_element(key, partition, element_spec, default),
    do: Ets.update_element(partition, key, element_spec, default)

  def update_element(key, partition, element_spec),
    do: Ets.update_element(partition, key, element_spec)

  @doc "Increment/decrement a counter field.  Optional default for missing keys."
  def update_counter(key, partition, counter_spec, default),
    do: Ets.update_counter(partition, key, counter_spec, default)

  def update_counter(key, partition, counter_spec),
    do: Ets.update_counter(partition, key, counter_spec)

  ## Read ##

  @doc "Look up records by key."
  @spec get(any, atom | :ets.tid()) :: [tuple]
  def get(key, partition), do: Ets.lookup(partition, key)

  @doc "Pattern match — returns list of bound variable lists (`:ets.match/2`)."
  @spec get_by_match(atom | tuple, atom | :ets.tid()) :: [[any]]
  def get_by_match(pattern, partition) do
    Logger.debug(fn -> "super_cache, storage, match pattern=#{inspect(pattern)} partition=#{partition}" end)
    Ets.match(partition, pattern)
  end

  @doc "Pattern match — returns full matching objects (`:ets.match_object/2`)."
  @spec get_by_match_object(atom | tuple, atom | :ets.tid()) :: [tuple]
  def get_by_match_object(pattern, partition) do
    Logger.debug(fn -> "super_cache, storage, match_object pattern=#{inspect(pattern)} partition=#{partition}" end)
    Ets.match_object(partition, pattern)
  end

  @doc "Fold over all records in a partition."
  @spec scan((any, any -> any), any, atom | :ets.tid()) :: any
  def scan(fun, acc, partition), do: Ets.foldl(fun, acc, partition)

  @doc "Remove and return a record by key (atomic take)."
  @spec take(any, atom | :ets.tid()) :: [tuple]
  def take(key, partition), do: Ets.take(partition, key)

  ## Delete ##

  @doc "Delete a record by key."
  @spec delete(any, atom | :ets.tid()) :: true
  def delete(key, partition), do: Ets.delete(partition, key)

  @doc "Delete all records in a partition."
  @spec delete_all(atom | :ets.tid()) :: true
  def delete_all(partition), do: Ets.delete_all_objects(partition)

  @doc "Delete all records matching `pattern`."
  @spec delete_match(atom | tuple, atom | :ets.tid()) :: true
  def delete_match(pattern, partition) do
    Logger.debug(fn -> "super_cache, storage, delete_match pattern=#{inspect(pattern)} partition=#{partition}" end)
    Ets.match_delete(partition, pattern)
  end

  ## Stats ##

  @doc "Return `{partition, record_count}` for a partition."
  @spec stats(atom | :ets.tid()) :: {atom | :ets.tid(), non_neg_integer}
  def stats(partition), do: {partition, Ets.info(partition, :size)}

  ## Private ##

  defp table_name(prefix, order), do: String.to_atom("#{prefix}_#{order}")
end
