defmodule SuperCache.EtsHolder do
  use GenServer, restart: :temporary
  require Logger

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the server.
  """
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: name)
  end

  def stop(name) do
   GenServer.call(name, :stop)
  end

  def clean(name) do
    GenServer.call(name, :clean)
   end

  # Server (callbacks)

  @impl true
  def init(table_name) do
    Logger.info("start process own ets cache table for #{inspect table_name}")
    ^table_name = :ets.new(table_name, [
      :set,
      :public,
      :named_table,
      {:write_concurrency, true},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ])
    Logger.info("table #{inspect table_name} is created")

    {:ok, table_name}
  end

  @impl true
  def handle_call(:stop, _from, table_name) do
    {:stop, :ok, table_name}
  end

  @impl true
  def handle_call(:clean, _from, table_name) do
    {:reply,  clean_up(table_name), table_name}
  end

  @impl true
  def terminate(_reason, _) do
    :stop
  end

  defp clean_up(table_name) do
    :ets.delete_all_objects(table_name)
  end
end
