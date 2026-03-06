defmodule SuperCache.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("super_cache, application, starting...")

    children = [
      {SuperCache.Config,                [key_pos: 0, partition_pos: 0]},
      {SuperCache.Sup,                   []},
      {SuperCache.Partition.Holder,      []},
      {SuperCache.EtsHolder,             SuperCache.EtsHolder},
      # Cluster components — safe to run even in single-node mode.
      {SuperCache.Cluster.Manager,       []},
      {SuperCache.Cluster.NodeMonitor,   []}
    ]

    opts = [strategy: :one_for_one, name: SuperCache.Supervisor]
    {:ok, pid} = Supervisor.start_link(children, opts)

    if Application.get_env(:super_cache, :auto_start, false) do
      Logger.info("super_cache, application, auto start cache...")
      SuperCache.Cluster.Bootstrap.start!(
        Application.get_all_env(:super_cache)
      )

      connect_peers()
    end

    {:ok, pid}
  end

  defp connect_peers() do
    peers = Application.get_env(:super_cache, :cluster_peers, [])
    Enum.each(peers, fn peer ->
      case Node.connect(peer) do
        true  -> Logger.info("super_cache, connected to #{inspect(peer)}")
        false -> Logger.warning("super_cache, could not connect to #{inspect(peer)}")
        :ignored -> Logger.warning("super_cache, node not alive, ignored #{inspect(peer)}")
      end
    end)
  end
end
