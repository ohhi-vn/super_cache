defmodule SuperCache.Partition.Holder do
  @moduledoc false

  use GenServer, restart: :transient, shutdown: 1_000

  require Logger

  alias :ets, as: Ets

  ## APIs ##

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the server.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stops GenServer (in case shutdown library).
  """
  @spec stop :: :ok
  def stop() do
   GenServer.call(__MODULE__, :stop)
  end

  @doc """
  Clears all partitions' info.
  """
  @spec clean :: any
  def clean() do
    GenServer.call(__MODULE__, :clean)
  end

  @doc """
  Sets key/value for partition.
  """
  @spec set(non_neg_integer) :: any
  def set(order) when is_integer(order) and (order >= 0) do
    GenServer.call(__MODULE__, {:set_partition, order})
  end

  @doc """
  Gets partition belong with key.
  """
  @spec get(any) :: atom
  def get(order) do
    # get direct from ets table
    [{_, partition}] = Ets.lookup(__MODULE__, order)
    partition
  end

  @doc """
  Gets all partitions.
  """
  @spec get_all :: [list]
  def get_all() do
    Ets.match(__MODULE__, {:_, :"$1"})
  end

  ## Callbacks ##

  @impl true
  @spec init(any) :: {:ok, %{table_name: SuperCache.Partition.Holder}}
  def init(_opts) do
    table_name = __MODULE__
    Logger.info("start process own ets cache table for #{inspect table_name}")
    state = %{table_name: table_name}

    ^table_name = Ets.new(table_name, [
      :set,
      :protected,
      :named_table,
      {:read_concurrency, true}
      ])

    Logger.info("table #{inspect table_name} is created")

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
    partition = String.to_atom("supercache_partition_#{order}")
    Logger.debug("add partition #{inspect partition} for order #{order}")
    Ets.insert(table_name, {order, partition})
    {:reply,  :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :stop
  end

  @spec clean_up(atom) :: true
  defp clean_up(table_name) do
    Ets.delete_all_objects(table_name)
  end
end
