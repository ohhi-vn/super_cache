

defmodule SuperCache.Storage do
  @moduledoc """
  Thin wrapper around `:ets` that provides the read/write/delete primitives
  used throughout SuperCache.

  All functions accept either an atom (named ETS table) or an `:ets.tid()`
  (anonymous table reference) as the `partition` argument.

  This module is intentionally low-level. Application code should go
  through the higher-level `SuperCache`, `SuperCache.KeyValue`,
  `SuperCache.Queue`, etc., not through this module directly.

  ## Key position

  The ETS `keypos` is set from `:key_pos` config during table creation
  (`EtsHolder.create_table/1`). The keypos is 1-based in ETS, so a
  `:key_pos` of `0` corresponds to `keypos: 1`.

  ## Concurrency

  All tables are created with `{:write_concurrency, true}` and
  `{:read_concurrency, true}`. Multiple processes can read and write
  the same partition concurrently without external locking, with the
  exception of structural mutations in `Queue` and `Stack` which use a
  soft application-level lock (see those modules).

  ## Example

      alias SuperCache.Storage

      # Assuming a table named :my_table already exists:
      Storage.put({:user, 1, "Alice"}, :my_table)
      Storage.get(:user, :my_table)
      # => [{:user, 1, "Alice"}]

      Storage.delete(:user, :my_table)
      Storage.get(:user, :my_table)
      # => []
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{EtsHolder, Config}
  alias :ets, as: Ets

  ## Lifecycle ──────────────────────────────────────────────────────────────────

  @doc """
  Create `num` ETS partitions, named `<prefix>_0` through `<prefix>_{num-1}`.

  Called by `Bootstrap.start!/1` during system startup.

  ## Examples

      SuperCache.Storage.start(4)
      # => :ok
  """
  @spec start(pos_integer) :: :ok
  def start(num) when is_integer(num) and num > 0 do
    prefix = Config.get_config(:table_prefix)

    for order <- 0..(num - 1) do
      table = table_name(prefix, order)
      EtsHolder.new_table(EtsHolder, table)
    end

    Logger.info("super_cache, storage, #{num} partition(s) created")
    :ok
  end

  @doc """
  Delete all `num` ETS partitions.

  Called by `Bootstrap.stop/0` during system shutdown.

  ## Examples

      SuperCache.Storage.stop(4)
      # => :ok
  """
  @spec stop(pos_integer) :: :ok
  def stop(num) when is_integer(num) and num > 0 do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, stopping #{num} partition(s)"
    end)

    prefix = Config.get_config(:table_prefix)

    for order <- 0..(num - 1) do
      name = table_name(prefix, order)
      SuperCache.Log.debug(fn ->
        "super_cache, storage, deleting table #{inspect(name)}"
      end)

      try do
        EtsHolder.delete_table(EtsHolder, name)
      catch
        :exit, _ -> :ok
      end
    end

    Logger.info("super_cache, storage, #{num} partition(s) deleted")
    :ok
  end

  ## Write ──────────────────────────────────────────────────────────────────────

  @doc """
  Insert one or more tuples into `partition`.

  For `:set` / `:ordered_set` tables, inserting a record whose key already
  exists overwrites the previous record.

  ## Examples

      Storage.put({:session, "tok-1", :active}, :my_table)
      Storage.put([{:a, 1}, {:b, 2}], :my_table)   # batch insert
  """
  @spec put([tuple] | tuple, atom | :ets.tid()) :: true
  def put(term, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, put into #{inspect(partition)}"
    end)

    Ets.insert(partition, term)
  end

  @doc """
  Insert `term` only if no record with the same key exists.

  Returns `true` on success, `false` if the key is already present.
  Only meaningful for `:set` / `:ordered_set` tables.

  Used by `Queue` and `Stack` as a compare-and-swap for initialisation.
  """
  @spec insert_new(tuple, atom | :ets.tid()) :: boolean
  def insert_new(term, partition), do: Ets.insert_new(partition, term)

  @doc """
  Update one or more fields in an existing record at `key`.

  `element_spec` follows the `:ets.update_element/3` convention:
  `{position, new_value}` or `[{position, new_value}, …]`.

  `default` is inserted as a new record when the key does not yet exist
  (requires the four-argument form).
  """
  def update_element(key, partition, element_spec, default),
    do: Ets.update_element(partition, key, element_spec, default)

  def update_element(key, partition, element_spec),
    do: Ets.update_element(partition, key, element_spec)

  @doc """
  Atomically increment or decrement a counter field.

  `counter_spec` follows the `:ets.update_counter/3` convention.
  `default` is inserted when the key does not yet exist (four-argument form).
  """
  def update_counter(key, partition, counter_spec, default),
    do: Ets.update_counter(partition, key, counter_spec, default)

  def update_counter(key, partition, counter_spec),
    do: Ets.update_counter(partition, key, counter_spec)

  ## Read ───────────────────────────────────────────────────────────────────────

  @doc """
  Look up all records with `key` in `partition`.

  Returns a list of tuples (empty when the key does not exist).

  ## Examples

      Storage.get(:user, :my_table)
      # => [{:user, 1, "Alice"}]

      Storage.get(:missing, :my_table)
      # => []
  """
  @spec get(any, atom | :ets.tid()) :: [tuple]
  def get(key, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, get key=#{inspect(key)} from #{inspect(partition)}"
    end)

    Ets.lookup(partition, key)
  end

  @doc """
  Pattern match using `:ets.match/2`.

  Returns a list of binding lists. Wildcards are `:_`; captures are
  `:"$1"`, `:"$2"`, etc.

  ## Examples

      Storage.get_by_match({:user, :"$1", :admin}, :my_table)
      # => [[1], [42]]  (ids of admin users)

      Storage.get_by_match({:session, :_, :expired}, :my_table)
      # => []  (no expired sessions)
  """
  @spec get_by_match(atom | tuple, atom | :ets.tid()) :: [[any]]
  def get_by_match(pattern, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, match pattern=#{inspect(pattern)} in #{inspect(partition)}"
    end)

    Ets.match(partition, pattern)
  end

  @doc """
  Pattern match using `:ets.match_object/2`.

  Returns full matching tuples rather than capture binding lists.

  ## Examples

      Storage.get_by_match_object({:user, :_, :admin}, :my_table)
      # => [{:user, 1, :admin}, {:user, 42, :admin}]

      Storage.get_by_match_object({:user, :_, :banned}, :my_table)
      # => []
  """
  @spec get_by_match_object(atom | tuple, atom | :ets.tid()) :: [tuple]
  def get_by_match_object(pattern, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, match_object pattern=#{inspect(pattern)} in #{inspect(partition)}"
    end)

    Ets.match_object(partition, pattern)
  end

  @doc """
  Fold over all records in `partition` using `:ets.foldl/3`.

  `fun/2` receives `(record, accumulator)` and must return the new
  accumulator.

  ## Examples

      Storage.scan(fn {_, score}, acc -> acc + score end, 0, :my_table)
      # => 1024  (sum of all scores)

      Storage.scan(fn _rec, acc -> acc + 1 end, 0, :my_table)
      # => 50  (count of records)
  """
  @spec scan((any, any -> any), any, atom | :ets.tid()) :: any
  def scan(fun, acc, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, scan on #{inspect(partition)}"
    end)

    Ets.foldl(fun, acc, partition)
  end

  @doc """
  Atomically remove and return the record at `key`.

  Returns `[tuple]` (empty when the key does not exist). The removal and
  the return are a single atomic ETS operation — no other process can
  observe the record between the read and the delete.

  Used by `Queue` and `Stack` for lock-free counter management.

  ## Examples

      Storage.take(:session_tok, :my_table)
      # => [{:session_tok, "active"}]

      Storage.take(:missing, :my_table)
      # => []
  """
  @spec take(any, atom | :ets.tid()) :: [tuple]
  def take(key, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, take key=#{inspect(key)} from #{inspect(partition)}"
    end)

    Ets.take(partition, key)
  end

  ## Delete ─────────────────────────────────────────────────────────────────────

  @doc """
  Delete the record at `key`.

  Always succeeds (no-op when the key does not exist).

  ## Examples

      Storage.delete(:session_tok, :my_table)
      # => true
  """
  @spec delete(any, atom | :ets.tid()) :: true
  def delete(key, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, delete key=#{inspect(key)} from #{inspect(partition)}"
    end)

    Ets.delete(partition, key)
  end

  @doc """
  Delete all records in `partition`.

  ## Examples

      Storage.delete_all(:my_table)
      # => true
  """
  @spec delete_all(atom | :ets.tid()) :: true
  def delete_all(partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, delete_all on #{inspect(partition)}"
    end)

    Ets.delete_all_objects(partition)
  end

  @doc """
  Delete all records matching `pattern` using `:ets.match_delete/2`.

  Pattern semantics are the same as `get_by_match/2`.

  ## Examples

      # Remove all expired sessions
      Storage.delete_match({:session, :_, :expired}, :my_table)
      # => true
  """
  @spec delete_match(atom | tuple, atom | :ets.tid()) :: true
  def delete_match(pattern, partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, storage, delete_match pattern=#{inspect(pattern)} in #{inspect(partition)}"
    end)

    Ets.match_delete(partition, pattern)
  end

  ## Stats ──────────────────────────────────────────────────────────────────────

  @doc """
  Return `{partition, record_count}` for `partition`.

  Uses `:ets.info(partition, :size)` which is an O(1) operation thanks
  to the `:decentralized_counters` option set during table creation.

  ## Examples

      Storage.stats(:my_table)
      # => {:my_table, 1024}

      Storage.stats(:nonexistent)
      # => {:nonexistent, :undefined}
  """
  @spec stats(atom | :ets.tid()) :: {atom | :ets.tid(), non_neg_integer | :undefined}
  def stats(partition) do
    size = Ets.info(partition, :size)
    {partition, size}
  end

  ## ── Private helpers ──────────────────────────────────────────────────────────

  @doc false
  defp table_name(prefix, order), do: String.to_atom("#{prefix}_#{order}")
end
