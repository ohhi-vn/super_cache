defmodule SuperCache.Application do
  @moduledoc """
  OTP Application callback module for SuperCache.

  Starts the core supervision tree, which includes:
  - `SuperCache.Config` — central configuration store
  - `SuperCache.Sup` — dynamic supervisor for user-spawned workers
  - `SuperCache.Partition.Holder` — partition registry
  - `SuperCache.EtsHolder` — ETS table lifecycle manager
  - `SuperCache.Cluster.Manager` — cluster membership & partition mapping
  - `SuperCache.Cluster.NodeMonitor` — node connectivity watcher
  - `SuperCache.Cluster.TxnRegistry` — 3PC transaction log
  - `SuperCache.Cluster.Metrics` — observability counter store

  ## Auto-start

  If `config :super_cache, auto_start: true` is set, the cache will
  automatically start during application boot using all values from the
  application environment.  Cluster peers listed under `:cluster_peers`
  will also be connected.

  ## Example

      # config/config.exs
      config :super_cache,
        auto_start: true,
        key_pos: 0,
        partition_pos: 0,
        num_partition: 8,
        cluster_peers: [:"node2@127.0.0.1", :"node3@127.0.0.1"]
  """

  use Application

  require Logger
  require SuperCache.Log

  @impl true
  def start(_type, _args) do
    Logger.info("super_cache, application, starting supervision tree...")

    children = [
      # Core config — must start first.
      {SuperCache.Config, [key_pos: 0, partition_pos: 0]},
      # Dynamic supervisor for user-spawned workers.
      {SuperCache.Sup, []},
      # Partition registry.
      {SuperCache.Partition.Holder, []},
      # Owns the ETS data tables.
      {SuperCache.EtsHolder, SuperCache.EtsHolder},
      # Cluster components — always started; idle in local mode.
      {SuperCache.Cluster.Manager, []},
      {SuperCache.Cluster.NodeMonitor, []},
      # 3PC transaction log — must start before Bootstrap.
      {SuperCache.Cluster.TxnRegistry, []},
      # Metrics store.
      {SuperCache.Cluster.Metrics, []}
    ]

    opts = [strategy: :one_for_one, name: SuperCache.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("super_cache, application, supervision tree started")

        if Application.get_env(:super_cache, :auto_start, false) do
          Logger.info("super_cache, application, auto-starting cache...")
          start_cache()
          connect_peers()
        end

        {:ok, pid}

      {:error, reason} ->
        Logger.error(
          "super_cache, application, failed to start supervision tree: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # ── Private helpers ──────────────────────────────────────────────────────────

  defp start_cache() do
    env = Application.get_all_env(:super_cache)

    try do
      SuperCache.Bootstrap.start!(env)
      Logger.info("super_cache, application, cache auto-started successfully")
    rescue
      err ->
        Logger.error(
          "super_cache, application, cache auto-start failed: #{inspect(err)}\n" <>
            "Stacktrace: #{inspect(__STACKTRACE__)}"
        )
    end
  end

  defp connect_peers() do
    peers = Application.get_env(:super_cache, :cluster_peers, [])

    if peers == [] do
      SuperCache.Log.debug(fn -> "super_cache, application, no cluster peers configured" end)
    else
      Logger.info("super_cache, application, connecting to #{length(peers)} cluster peer(s)...")
    end

    results =
      Enum.map(peers, fn peer ->
        case Node.connect(peer) do
          true ->
            Logger.info("super_cache, application, connected to peer #{inspect(peer)}")
            {:ok, peer}

          false ->
            Logger.warning(
              "super_cache, application, failed to connect to peer #{inspect(peer)} " <>
                "(node may be down or unreachable)"
            )

            {:error, :connection_failed}

          :ignored ->
            Logger.warning(
              "super_cache, application, peer connection ignored for #{inspect(peer)} " <>
                "(current node is not running in distributed mode)"
            )

            {:error, :not_distributed}
        end
      end)

    successes = Enum.count(results, &match?({:ok, _}, &1))
    failures = Enum.count(results, &match?({:error, _}, &1))

    if failures > 0 do
      Logger.warning(
        "super_cache, application, peer connection summary: " <>
          "#{successes} succeeded, #{failures} failed"
      )
    else
      Logger.info("super_cache, application, all #{successes} peer(s) connected successfully")
    end

    :ok
  end
end
