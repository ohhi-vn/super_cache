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

    deadline = :erlang.monotonic_time(:millisecond) + timeout

    result =
      submit_with_retry(server, partition_idx, command, timeout, deadline, 3)

    case result do
      {:ok, :ok, _leader} ->
        {:ok, :ok}

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

  # Bounded retry for leader-placement failures only. Ambiguous timeouts are
  # never resubmitted because the command may already be committed. Durable-write
  # failures (:wal_down, {:not_appended, ...}) and bounded-queue backpressure
  # (:backpressure) are terminal: the `other` clause below returns them without
  # retry, since retrying would risk duplicates or hide overload.
  defp submit_with_retry(_server, _partition_idx, _command, _timeout, _deadline, attempts)
       when attempts <= 0 do
    {:error, :no_leader}
  end

  defp submit_with_retry(server, partition_idx, command, timeout, deadline, attempts) do
    if :erlang.monotonic_time(:millisecond) >= deadline or attempts <= 0 do
      {:error, :no_leader}
    else
      remaining = max(1, deadline - :erlang.monotonic_time(:millisecond))
      call_timeout = min(timeout, remaining)

      case submit_once(server, partition_idx, command, call_timeout) do
        {:ok, reply, leader} ->
          normalize_reply(reply, leader)

        {:error, {:not_leader, leader}} ->
          retry_with_leader(
            {Group.name(partition_idx), leader},
            partition_idx,
            command,
            timeout,
            deadline,
            attempts
          )

        {:error, reason} when reason in [:not_leader, :no_leader, :noproc] ->
          handle_no_leader(server, partition_idx, command, timeout, deadline, attempts, reason)

        {:error, {:exit, {:nodedown, _}}} ->
          handle_nodedown(partition_idx, command, timeout, deadline, attempts)

        other ->
          other
      end
    end
  end

  defp retry_with_leader(server, partition_idx, command, timeout, deadline, attempts) do
    if attempts > 1 do
      submit_with_retry(server, partition_idx, command, timeout, deadline, attempts - 1)
    else
      {:error, :not_leader}
    end
  end

  defp handle_no_leader(server, partition_idx, command, timeout, deadline, attempts, reason) do
    if reason == :no_leader do
      maybe_reconcile_and_retry(server, partition_idx, command, timeout, deadline, attempts)
    else
      case Adapter.find_leader(Group.cluster_name(partition_idx)) do
        {:ok, leader} when attempts > 1 ->
          submit_with_retry(
            {Group.name(partition_idx), leader},
            partition_idx,
            command,
            timeout,
            deadline,
            attempts - 1
          )

        _ ->
          maybe_reconcile_and_retry(server, partition_idx, command, timeout, deadline, attempts)
      end
    end
  end

  defp handle_nodedown(partition_idx, command, timeout, deadline, attempts) do
    case Adapter.find_leader(Group.cluster_name(partition_idx)) do
      {:ok, leader} when attempts > 1 ->
        submit_with_retry(
          {Group.name(partition_idx), leader},
          partition_idx,
          command,
          timeout,
          deadline,
          attempts - 1
        )

      _ ->
        {:error, {:exit, {:nodedown, :node}}}
    end
  end

  defp maybe_reconcile_and_retry(_server, partition_idx, command, timeout, deadline, attempts) do
    {primary, _replicas} = Manager.get_replicas(partition_idx)

    if primary == node() and System.running?() and attempts > 1 do
      _ = Reconciler.reconcile(partition_idx)

      submit_with_retry(
        server_for(partition_idx),
        partition_idx,
        command,
        timeout,
        deadline,
        attempts - 1
      )
    else
      {:error, :no_leader}
    end
  end

  defp normalize_reply(:ok, leader), do: {:ok, :ok, leader}
  defp normalize_reply({:ok, :already_applied}, leader), do: {:ok, :ok, leader}
  defp normalize_reply({:error, reason}, _leader), do: {:error, reason}
  defp normalize_reply(other, leader), do: {:ok, other, leader}

  defp submit_once(server, _partition_idx, command, timeout) do
    case Adapter.process_command(server, command, timeout) do
      {:error, {:not_appended, reason, _, _}} ->
        {:error, reason}

      {:error, {:not_appended, _} = not_appended} ->
        {:error, not_appended}

      result ->
        result
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
