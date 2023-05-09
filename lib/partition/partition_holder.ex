defmodule SuperCache.Partition.Holder do
  use GenServer
  require Logger

  alias :ets, as: Ets

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the server.
  """
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: __MODULE__)
  end

  def stop() do
   GenServer.call(__MODULE__, :stop)
  end

  def clean() do
    GenServer.call(__MODULE__, :clean)
  end

  def set(order) when is_integer(order) and (order >= 0) do
    GenServer.call(__MODULE__, {:set_partition, order})
  end

  def get(order) do
    # get direct from ets table
    [{_, partition}] = Ets.lookup(__MODULE__, order)
    partition
  end

  # Server (callbacks)

  @impl true
  def init(opts) do
    if !Keyword.has_key?(opts, :table_name) do
      raise "missed table_name in parameters"
    end
    table_name = __MODULE__ #Keyword.get(opts, :table_name)
    Logger.info("start process own ets cache table for #{inspect table_name}")
    state = %{table_name: table_name}

    fun =
      fn ->
        ^table_name = Ets.new(table_name, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true}
          ])

        Ets.insert(table_name, {__MODULE__, self()})
        Logger.info("table #{inspect table_name} is created")
        # suspend process
        Process.sleep(:infinity)
      end

    ets_pid =
      case Ets.whereis(table_name) do
        :undefined -> # create new table
          spawn(fun)
        _ ->
          [pid] = Ets.lookup(table_name, __MODULE__)
          pid
      end

    state = Map.put(state, :ets_pid, ets_pid)

    {:ok, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    {:stop, :ok, state}
  end

  @impl true
  def handle_call(:clean, _from, state) do
    %{table_name: table_name} = state
    {:reply,  clean_up(table_name), state}
  end

  @impl true
  def handle_call( {:set_partition, order}, _from, state) do
    %{table_name: table_name} = state
    partition = String.to_atom("partition_#{order}")
    Logger.debug("add partition #{inspect partition} for order #{order}")
    Ets.insert(table_name, {order, partition})
    {:reply,  :ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    %{ets_pid: pid} = state
    # kill process owner ets to remove ets.
    Process.exit(pid, :shutdown)

    :stop
  end

  defp clean_up(table_name) do
    Ets.delete_all_objects(table_name)
  end
end
