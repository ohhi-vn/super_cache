defmodule SuperCache.Cluster.Raft.System do
  @moduledoc """
  Lifecycle facade for the RaftEx system used by SuperCache's `:raft` mode.

  The system is started lazily by cluster bootstrap, after partitions and
  membership are available. Only one system is used for the whole node; each
  partition is a separate Raft cluster (server group) inside it.
  """

  require Logger

  alias SuperCache.Cluster.Manager
  alias SuperCache.Cluster.Raft.{Adapter, Config, Group}

  @system_name :super_cache_raft
  @start_key {__MODULE__, :started}

  @doc "Return the RaftEx system name used by SuperCache."
  @spec system_name() :: atom
  def system_name, do: @system_name

  @doc "Return the server id for a partition on this node."
  @spec server_id(non_neg_integer) :: {atom, node}
  def server_id(partition_idx), do: Group.server_id(partition_idx)

  @doc "Return whether the Raft system has been started."
  @spec running?() :: boolean
  def running?, do: :persistent_term.get(@start_key, false)

  @doc "Return the RaftEx system configuration for this node."
  @spec system_config() :: map
  def system_config do
    config = Config.all()

    RaftEx.App.System.default_config()
    |> Map.put(:name, @system_name)
    |> Map.put(:data_dir, Keyword.fetch!(config, :data_dir))
    |> Map.put(:wal_data_dir, Keyword.fetch!(config, :data_dir))
    |> Map.put(:wal_sync_method, Keyword.fetch!(config, :wal_sync_method))
    |> Map.put(:wal_max_size_bytes, Keyword.fetch!(config, :wal_max_size_bytes))
    |> Map.put(:election_timeout_min, Keyword.fetch!(config, :election_timeout_min))
    |> Map.put(:election_timeout_max, Keyword.fetch!(config, :election_timeout_max))
    |> Map.put(:heartbeat_interval, Keyword.fetch!(config, :heartbeat_interval))
    |> Map.put(:segment_max_entries, Keyword.fetch!(config, :segment_max_entries))
    |> Map.put(:segment_max_size_bytes, Keyword.fetch!(config, :segment_max_size_bytes))
    |> Map.put(:names, Adapter.derive_names(@system_name))
  end

  @doc "Start the RaftEx system and create groups for locally owned partitions."
  @spec start!() :: :ok
  def start! do
    :ok = Config.validate!()

    if not running?() do
      Logger.info("super_cache, raft.system, starting RaftEx system")

      case Adapter.system_start(system_config()) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
        other -> raise "super_cache, raft.system, failed to start system: #{inspect(other)}"
      end

      await_ready()
      :persistent_term.put(@start_key, true)
    end

    Group.start_all()

    # Membership may have changed while the system was starting; bring each
    # local group in line with the current partition map before accepting writes.
    Group.local_partitions()
    |> Enum.filter(fn idx ->
      {primary, _replicas} = Manager.get_replicas(idx)
      primary == node()
    end)
    |> Enum.each(&Group.Reconciler.reconcile/1)

    :ok
  end

  @doc "Stop all local groups and the RaftEx system."
  @spec stop() :: :ok
  def stop do
    if running?() do
      # Stopping the system supervisor tears down all server children; clear
      # the group tracking afterwards so no stale ids remain.
      case Adapter.system_stop(@system_name) do
        :ok -> :ok
        {:error, :not_found} -> :ok
      end

      Group.untrack_all()
      Process.sleep(20)
      :persistent_term.erase(@start_key)
    end

    :ok
  end

  # RaftEx's system start is synchronous, but its child supervisors register
  # their names during init; poll briefly so callers never race the first
  # command against a half-started system.
  defp await_ready(timeout \\ 5_000) do
    await_ready_loop(System.monotonic_time(:millisecond) + timeout)
  end

  defp await_ready_loop(deadline) do
    names = Adapter.derive_names(@system_name)

    if Process.whereis(Map.fetch!(names, :server_sup)) != nil do
      :ok
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining > 0 do
        Process.sleep(10)
        await_ready_loop(deadline)
      else
        raise "super_cache, raft.system, RaftEx system did not become ready"
      end
    end
  end
end
