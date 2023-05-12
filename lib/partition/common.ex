defmodule SuperCache.Partition.Common do
  @moduledoc false

  use GenServer

  require Logger

  ## APIs ##

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    Logger.info("start #{inspect __MODULE__} with opts: #{inspect opts}")
    GenServer.start_link(__MODULE__, [opts], name: __MODULE__)
  end

  @doc """
  Gets number of partition in cache.
  """
  @spec get_num_partition :: pos_integer
  def get_num_partition() do
    GenServer.call(__MODULE__, :get_num_partition, 1000)
  end

  @doc """
  Gets number of online scheduler of VM.
  """
  @spec get_schedulers :: pos_integer
  def get_schedulers() do
    System.schedulers_online()
  end

  @doc """
  Sets number of paritions.
  """
  @spec set_num_partition(pos_integer()) :: any
  def set_num_partition(:schedulers_online) do
    GenServer.call(__MODULE__, {:set_num_partition, get_schedulers()}, 1000)
  end
  def set_num_partition(num) do
    GenServer.call(__MODULE__, {:set_num_partition, num}, 1000)
  end

  @doc """
  Gets hash for data.
  """
  @spec get_hash(any) :: non_neg_integer
  def get_hash(term) do
    :erlang.phash2(term)
  end

  @doc """
  Gets order of partition from data.
  """
  @spec get_pattition_order(any) :: non_neg_integer
  def get_pattition_order(term) do
    num = get_num_partition()
    :erlang.phash2(term, num)
  end

  ## Callbacks ###

  @impl GenServer
  @spec init(keyword) :: {:ok, %{num_partitiion: pos_integer}}
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
