defmodule SuperCache.Partition.Common do
  @moduledoc """
  Documentation for `SuperCache`.
  """
  use GenServer

  require Logger

  ## APIs ##

  def start_link(opts) do
    Logger.info("start #{inspect __MODULE__} with opts: #{inspect opts}")
    GenServer.start_link(__MODULE__, [opts], name: __MODULE__)
  end

  def get_num_partition() do
    GenServer.call(__MODULE__, :get_num_partition, 1000)
  end

  def get_schedulers() do
    System.schedulers_online()
  end

  def set_num_partition(:schedulers_online) do
    GenServer.call(__MODULE__, {:set_num_partition, get_schedulers()}, 1000)
  end
  def set_num_partition(num) do
    GenServer.call(__MODULE__, {:set_num_partition, num}, 1000)
  end

  def get_hash(term) do
    :erlang.phash2(term)
  end

  def get_pattition_order(term) do
    num = get_num_partition()
    :erlang.phash2(term, num)
  end

  ## Callbacks ###

  @impl GenServer
  def init(opts) do
    num =
      case Keyword.get(opts, :fixed_num_partition) do
        nil -> # num_partition = number of online schedule of node
          System.schedulers_online()
        n when is_integer(n) and n > 0 ->
          n
      end
    Logger.info("start #{inspect __MODULE__} process done")
    {:ok, %{num_partitiion: num}}
  end

  @impl GenServer
  def handle_call(:get_num_partition, _from, %{num_partition: num} = state) do
    {:reply, num, state}
  end

  @impl GenServer
  def handle_call({:set_num_partition, num}, _from, state) do
    case num do
      n when is_integer(n) and (n > 0) ->
        {:reply, :ok, Map.put(state, :num_partition, num)}
      _ ->
        {:reply, {:error, :incorrect_param}, state}
    end

  end
end
