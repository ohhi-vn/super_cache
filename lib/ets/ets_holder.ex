defmodule SuperCache.EtsHolder do
  @moduledoc false

  use GenServer, restart: :temporary
  require Logger

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

  @doc """
  Clear all data in Ets table of GenServer.
  """
  @spec clean(atom | pid | {atom, any} | {:via, atom, any}) :: any
  def clean(name) do
    GenServer.call(name, :clean)
   end

  ### Callbacks ###

  @impl true
  @spec init(atom) :: {:ok, atom, :hibernate}
  def init(table_name) do
    Logger.info("start process own ets cache table for #{inspect table_name}")
    key_pos = Config.get_config(:key_pos) + 1 # key order of ets start from 1
    table_type = Config.get_config(:table_type)

    ^table_name = :ets.new(table_name, [
      table_type,
      :public,
      :named_table,
      {:keypos, key_pos},
      {:write_concurrency, true},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ])
    Logger.info("table #{inspect table_name} is created")

    {:ok, table_name, :hibernate}
  end

  @impl true
  def handle_call(:stop, _from, table_name) do
    {:stop, :normal, :ok, table_name}
  end

  @impl true
  def handle_call(:clean, _from, table_name) do
    {:reply,  clean_up(table_name), table_name}
  end

  @impl true
  def terminate(reason, name) do
    Logger.debug("#{inspect name} shutdown with reason #{inspect reason}")
    :stop
  end

  defp clean_up(table_name) do
    :ets.delete_all_objects(table_name)
  end
end
