defmodule SuperCache.ClusterMan do
  @moduledoc false

  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger

  # alias __MODULE__

  ## APIs ##

  def start_link(opts) do
    GenServer.start_link(__MODULE__ , opts, name: __MODULE__)
  end

  def add_nodes(node) when is_atom(node) do
    add_nodes([node])
  end
  def add_nodes(nodes) when is_list(nodes) do
    GenServer.call(__MODULE__, {:add_nodes, nodes})
  end

  ## callbacks ##

  @impl true
  def init(_opts) do
    Logger.info("starting ClusterMan")

    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_nodes, _nodes}, _from, state) do

    {:reply, {:error, "unimplemented"}, state}
  end
end
