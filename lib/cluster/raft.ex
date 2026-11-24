defmodule SuperCache.Cluster.Raft do
  @moduledoc """
  Public façade for the opt-in Raft replication mode.

  Every direct `raft_ex` call in SuperCache goes through this module (or the
  modules under `SuperCache.Cluster.Raft`), keeping the experimental upstream
  API behind one boundary. Commands are submitted to a partition's Raft group
  and only report success once the entry is committed and applied.
  """

  require Logger

  alias SuperCache.Cluster.{Error, Manager, Rpc}
  alias SuperCache.Cluster.Raft.{Adapter, Config, Group, System}
  alias SuperCache.Cluster.Raft.Group.Reconciler

  @doc "Submit a command to a partition's Raft group."
  @spec submit(non_neg_integer, term, keyword) :: {:ok, term} | {:error, Error.t()}
  def submit(partition_idx, command, opts \\ []) do
    if System.running?(), do: Group.ensure_started(partition_idx)

    timeout = Keyword.get(opts, :timeout, Config.get(:command_timeout_ms))
    operation = Keyword.get(opts, :operation, :put)
    server = server_for(partition_idx)

    context = [
      operation: operation,
      partition: partition_idx,
      node: elem(server, 1),
      mode: :raft
    ]

    case submit_with_retry(server, partition_idx, command, timeout, 1) do
      {:ok, reply, _leader} ->
        {:ok, reply}

      {:error, reason} ->
        {:error, Rpc.normalize(reason, context)}

      {:timeout, server_id} ->
        {:error, Rpc.normalize(:timeout, Keyword.put(context, :node, elem(server_id, 1)))}

      other ->
        {:error, Rpc.normalize(other, context)}
    end
  end

  # A group can lose its leader between membership events. Reconcile once and
  # retry rather than failing a write that the partition's primary can recover.
  defp submit_with_retry(server, partition_idx, command, timeout, retries) do
    case submit_once(server, partition_idx, command, timeout, 0) do
      {:error, :no_leader} when retries > 0 ->
        {primary, _replicas} = Manager.get_replicas(partition_idx)

        if primary == node() and System.running?() do
          _ = Reconciler.reconcile(partition_idx)

          submit_with_retry(
            server_for(partition_idx),
            partition_idx,
            command,
            timeout,
            retries - 1
          )
        else
          {:error, :no_leader}
        end

      other ->
        other
    end
  end

  defp submit_once(server, partition_idx, command, timeout, attempts) do
    case Adapter.process_command(server, command, timeout) do
      {:ok, _reply, _leader} = success ->
        success

      {:error, {:not_leader, leader}} when attempts == 0 ->
        submit_once({Group.name(partition_idx), leader}, partition_idx, command, timeout, 1)

      {:error, :not_leader} when attempts == 0 ->
        case Adapter.find_leader(Group.cluster_name(partition_idx)) do
          {:ok, leader} ->
            submit_once({Group.name(partition_idx), leader}, partition_idx, command, timeout, 1)

          _ ->
            {:error, :no_leader}
        end

      other ->
        other
    end
  rescue
    error ->
      Logger.warning("super_cache, raft, command failed at #{inspect(server)}: #{inspect(error)}")
      {:error, {:exit, error}}
  end

  defp server_for(partition_idx) do
    {primary, replicas} = Manager.get_replicas(partition_idx)

    if node() in [primary | replicas] do
      Group.server_id(partition_idx)
    else
      {Group.name(partition_idx), primary}
    end
  end
end
