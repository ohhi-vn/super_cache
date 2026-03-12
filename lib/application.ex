defmodule SuperCache.Application do
  @moduledoc false

  use Application
  require Logger
  require SuperCache.Log

  @impl true
  def start(_type, _args) do
    Logger.info("super_cache, application, starting...")

    children = [
      # Core config — must start first.
      {SuperCache.Config,              [key_pos: 0, partition_pos: 0]},
      # Dynamic supervisor for user-spawned workers.
      {SuperCache.Sup,                 []},
      # Partition registry.
      {SuperCache.Partition.Holder,    []},
      # Owns the ETS data tables.
      {SuperCache.EtsHolder,           SuperCache.EtsHolder},
      # Cluster components — always started; idle in local mode.
      {SuperCache.Cluster.Manager,     []},
      {SuperCache.Cluster.NodeMonitor, []},
      # 3PC transaction log — must start before Bootstrap.
      {SuperCache.Cluster.TxnRegistry, []},
      # Metrics store.
      {SuperCache.Cluster.Metrics,     []}
    ]

    opts = [strategy: :one_for_one, name: SuperCache.Supervisor]
    {:ok, pid} = Supervisor.start_link(children, opts)

    if Application.get_env(:super_cache, :auto_start, false) do
      Logger.info("super_cache, application, auto-starting cache...")
      SuperCache.Bootstrap.start!(Application.get_all_env(:super_cache))
      connect_peers()
    end

    {:ok, pid}
  end

  defp connect_peers() do
    Application.get_env(:super_cache, :cluster_peers, [])
    |> Enum.each(fn peer ->
      case Node.connect(peer) do
        true     -> Logger.info("super_cache, application, connected to #{inspect(peer)}")
        false    -> Logger.warning("super_cache, application, could not connect to #{inspect(peer)}")
        :ignored -> Logger.warning("super_cache, application, node not distributed, ignored #{inspect(peer)}")
      end
    end)
  end
end
