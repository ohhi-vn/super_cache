defmodule SuperCache.EtsHolder do
  @moduledoc """
  GenServer owner for ETS tables managed by SuperCache.

  This process owns the lifecycle of all ETS tables used for caching.
  Tables are created, tracked, and deleted through this GenServer so that:

  - Tables are automatically deleted when the owning process terminates.
  - The table list is tracked in state for clean shutdown.
  - Creation respects the global `:key_pos` and `:table_type` configuration.

  ## Lifecycle

  1. `start_link/1` starts the GenServer under a given name.
  2. `new_table/2` creates a new ETS table with the configured options.
  3. `delete_table/2` removes a specific table.
  4. On shutdown, `terminate/2` deletes all tracked tables to free memory.

  ## Configuration

  Tables are created with the following options:
  - `:keypos` — derived from `Config.get_config(:key_pos) + 1` (ETS is 1-based)
  - `:table_type` — from `Config.get_config(:table_type)` (`:set`, `:bag`, etc.)
  - `:public` — accessible by any process
  - `:named_table` — addressable by atom name
  - `{:write_concurrency, true}` — optimised for concurrent writes
  - `{:read_concurrency, true}` — optimised for concurrent reads
  - `{:decentralized_counters, true}` — reduces contention on counter updates

  ## Example

      {:ok, pid} = SuperCache.EtsHolder.start_link(:my_ets_owner)
      SuperCache.EtsHolder.new_table(:my_ets_owner, :my_cache_table)
      :ets.insert(:my_cache_table, {:key, "value"})
      SuperCache.EtsHolder.stop(:my_ets_owner)
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger
  require SuperCache.Log

  alias SuperCache.Config

  ## ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Starts the EtsHolder GenServer under the given `name`.

  The process enters hibernation when idle to reduce memory footprint.
  """
  @spec start_link(atom()) :: :ignore | {:error, any} | {:ok, pid()}
  def start_link(name) when is_atom(name) do
    GenServer.start_link(__MODULE__, name, name: name)
  end

  @doc """
  Stops the EtsHolder GenServer and deletes all owned ETS tables.

  The `terminate/2` callback ensures all tracked tables are removed
  before the process exits.
  """
  @spec stop(atom() | pid() | {atom(), any()} | {:via, atom(), any()}) :: :ok
  def stop(name) do
    GenServer.call(name, :stop, 5_000)
  end

  @doc """
  Creates a new named ETS table owned by this GenServer.

  The table is configured according to the current `:key_pos` and
  `:table_type` settings in `SuperCache.Config`.

  ## Examples

      SuperCache.EtsHolder.new_table(:my_owner, :users_cache)
      # => :ok
  """
  @spec new_table(atom(), atom()) :: :ok
  def new_table(name, table_name) do
    SuperCache.Log.debug(fn ->
      "super_cache, ets_holder, creating table #{inspect(table_name)}"
    end)

    GenServer.call(name, {:new, table_name}, 5_000)
  end

  @doc """
  Deletes a specific ETS table tracked by this GenServer.

  The table is removed from ETS and from the internal tracking list.
  No-op if the table is not found.
  """
  @spec delete_table(atom(), atom()) :: :ok
  def delete_table(name, table_name) do
    SuperCache.Log.debug(fn ->
      "super_cache, ets_holder, deleting table #{inspect(table_name)}"
    end)

    GenServer.call(name, {:delete, table_name}, 5_000)
  end

  @doc """
  Clears all records from a specific ETS table without deleting it.

  Returns `true` on success.
  """
  @spec clean(atom(), atom()) :: true
  def clean(name, table_name) do
    SuperCache.Log.debug(fn ->
      "super_cache, ets_holder, clearing table #{inspect(table_name)}"
    end)

    GenServer.call(name, {:clean, table_name}, 5_000)
  end

  @doc """
  Clears all records from all tracked ETS tables.

  Tables are not deleted — only their contents are removed.
  """
  @spec clean_all(atom()) :: :ok
  def clean_all(name) do
    GenServer.call(name, :clean_all, 5_000)
  end

  ## ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(name) do
    Logger.info("super_cache, ets_holder, started (name: #{inspect(name)})")
    {:ok, %{my_name: name, table_list: []}, :hibernate}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    Logger.info(
      "super_cache, ets_holder, stopping (#{length(state.table_list)} table(s) tracked)"
    )

    {:stop, :normal, :ok, state}
  end

  def handle_call({:clean, table_name}, _from, state) do
    {:reply, clean_up(table_name), state}
  end

  def handle_call(:clean_all, _from, %{table_list: tables} = state) do
    count = length(tables)

    Enum.each(tables, fn table ->
      SuperCache.Log.debug(fn ->
        "super_cache, ets_holder, clearing #{inspect(table)}"
      end)

      clean_up(table)
    end)

    Logger.info("super_cache, ets_holder, cleared #{count} table(s)")
    {:reply, :ok, state}
  end

  def handle_call({:new, table_name}, _from, %{table_list: tables} = state) do
    create_table(table_name)

    Logger.info(
      "super_cache, ets_holder, table #{inspect(table_name)} created (#{length(tables) + 1} total)"
    )

    {:reply, :ok, %{state | table_list: [table_name | tables]}, :hibernate}
  end

  def handle_call({:delete, table_name}, _from, %{table_list: tables} = state) do
    if table_name in tables do
      # Guard against the table already being deleted (e.g., by a prior stop
      # or the create_table cleanup path) — :ets.delete raises on missing tables.
      case :ets.info(table_name) do
        :undefined ->
          SuperCache.Log.debug(fn ->
            "super_cache, ets_holder, table #{inspect(table_name)} already gone, skipping delete"
          end)

        _ ->
          :ets.delete(table_name)
          Logger.info("super_cache, ets_holder, table #{inspect(table_name)} deleted")
      end
    else
      SuperCache.Log.debug(fn ->
        "super_cache, ets_holder, table #{inspect(table_name)} not found, skipping delete"
      end)
    end

    {:reply, :ok, %{state | table_list: List.delete(tables, table_name)}, :hibernate}
  end

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state, :hibernate}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state, :hibernate}
  end

  @impl true
  def terminate(reason, %{my_name: name, table_list: tables}) do
    count = length(tables)

    Logger.info(
      "super_cache, ets_holder, #{inspect(name)} shutting down " <>
        "(reason: #{inspect(reason)}, cleaning up #{count} table(s))"
    )

    Enum.each(tables, fn table ->
      try do
        :ets.delete(table)
      catch
        kind, err ->
          Logger.warning(
            "super_cache, ets_holder, failed to delete table #{inspect(table)}: " <>
              inspect({kind, err})
          )
      end
    end)

    :ok
  end

  def terminate(_reason, _state) do
    :ok
  end

  ## ── Private helpers ──────────────────────────────────────────────────────────

  defp clean_up(table_name) do
    :ets.delete_all_objects(table_name)
  end

  defp create_table(table_name) do
    key_pos = Config.get_config(:key_pos, 0) + 1
    table_type = Config.get_config(:table_type, :set)

    # If the table already exists (e.g., from a previous test or incomplete
    # shutdown), delete it first to avoid "table name already exists" errors.
    case :ets.info(table_name) do
      :undefined -> :ok
      _ -> :ets.delete(table_name)
    end

    ^table_name =
      :ets.new(table_name, [
        table_type,
        :public,
        :named_table,
        {:keypos, key_pos},
        {:write_concurrency, true},
        {:read_concurrency, true},
        {:decentralized_counters, true}
      ])

    :ok
  end
end
