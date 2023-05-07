defmodule SuperCache.Common do
  @moduledoc """
  Documentation for `SuperCache`.
  """
  use GenServer

  require Logger

  ## APIs ##

  def get_range() do
    GenServer.call(__MODULE__, :get_range, 1000)
  end

  def get_schedulers() do
    :erlang.system_info(:schedulers_online)
  end

  def start_link(opts) do
    Logger.info("start #{inspect __MODULE__} with opts: #{inspect opts}")
    GenServer.start_link(__MODULE__, [opts], name: __MODULE__)
  end

  ## Callbacks ###

  @impl GenServer
  @spec init(keyword) :: {:ok, %{range: pos_integer}}
  def init(opts) do
    range =
    case Keyword.get(opts, :fixed_range) do
      nil -> # range = number of online schedule of node
        :erlang.system_info(:schedulers_online)
      n when is_integer(n) and n > 0 ->
        n
    end
    Logger.info("start #{inspect __MODULE__} process done")
    {:ok, %{range: range}}
  end

  @impl GenServer
  def handle_call(:get_range, _from, %{range: range} = state) do
    {:reply, range, state}
  end

end
