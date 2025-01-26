defmodule SuperCache.EtsHolder do
  @moduledoc false

  use GenServer, restart: :permanent
  require Logger

  alias :ets, as: Ets
  alias SuperCache.Config

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the GenServer owner Ets table. The process start then go to hibernate.
  """
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: name)
  end

  @doc """
  Stops GenServer and delete Ets table.
  """
  @spec stop(atom | pid | {atom, any} | {:via, atom, any}) :: any
  def stop(name) do
   GenServer.call(name, :stop)
  end

  def new_table(name, table_name) do
    Logger.debug("super_cache, ets holder, new Ets table: #{inspect table_name}")
    GenServer.call(name, {:new, table_name})
  end

  def delete_table(name, table_name) do
    Logger.debug("super_cache, ets holder, delete Ets table: #{inspect table_name}")
    GenServer.call(name, {:delete, table_name})
  end

  @doc """
  Clear all data in Ets table of GenServer.
  """

  def clean(name, table_name) do
    Logger.debug("super_cache, ets holder, clean Ets table: #{inspect table_name}")
    GenServer.call(name, {:clean, table_name})
  end

  def clean_all(name) do
    GenServer.call(name, :clean_all)
  end

  ### Callbacks ###

  @impl true
  def init(name) do
    {:ok, %{my_name: name, table_list: []}}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:clean, table_name}, _from, state) do
    {:reply,  clean_up(table_name), state}
  end

  def handle_call(:clean_all, _from, state) do
    tables = Map.get(state, :table_list)

    for table <- tables do
      clean_up(table)
    end

    {:reply, :ok, state}
  end

  def handle_call({:new, table_name}, _from, state) do
    create_table(table_name)

    {:reply, :ok,  Map.update!(state, :table_list, &([table_name | &1]))}
  end

  def handle_call({:delete, table_name}, _from, state) do
    Ets.delete(table_name)

    {:reply, :ok,  Map.update!(state, :table_list, &([table_name | &1]))}
  end

  @impl true
  def terminate(reason, %{my_name: name} = state) do
    Logger.debug("super_cache, ets holder, #{inspect name} shutdown with reason #{inspect reason}")
    tables = Map.get(state, :table_list)

    for table <- tables do
      Ets.delete(table)
    end

    :stop
  end

  ## Private functions ##

  defp clean_up(table_name) do
    :ets.delete_all_objects(table_name)
  end

  defp create_table(table_name) do
    Logger.info("super_cache, ets holder, create cache table for #{inspect table_name}")
    key_pos = Config.get_config(:key_pos) + 1 # key order of ets start from 1
    table_type = Config.get_config(:table_type)

    ^table_name = Ets.new(table_name, [
      table_type,
      :public,
      :named_table,
      {:keypos, key_pos},
      {:write_concurrency, true},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ])
    Logger.info("super_cache, ets holder, table #{inspect table_name} is created")
    :ok
  end
end
