defmodule SuperCache.Cluster.NodeMonitor do
  @moduledoc """
  Monitors Erlang node connectivity and notifies `SuperCache.Cluster.Manager`
  when nodes join or leave so that partition maps can be updated.
  """
  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger

  alias SuperCache.Cluster.Manager, as: ClusterManager

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    :net_kernel.monitor_nodes(true, node_type: :all)
    Logger.info("super_cache, node_monitor, watching cluster events")
    {:ok, %{}}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("super_cache, node_monitor, node up: #{inspect(node)}")
    ClusterManager.node_up(node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("super_cache, node_monitor, node down: #{inspect(node)}")
    ClusterManager.node_down(node)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
