defmodule SuperCache.Cluster.Raft.Leadership do
  @moduledoc """
  Graceful leadership handoff for a planned partition primary move.

  The helper only acts on an explicit, planned change: it resolves the group's
  current leader, verifies that the incoming node is a caught-up voter, and
  asks the leader to hand over. Any failure is returned to the caller so the
  ownership path can fall back to the normal form/reconcile + election flow.
  """

  require Logger

  alias SuperCache.Cluster.Raft.{Adapter, Group, System}

  @timeout 10_000

  @doc """
  Transfer leadership of `partition_idx` to `target_node`, the new primary.

  Returns `:ok` when the target already leads or the handover succeeded,
  `:not_needed` when no local Raft system is running, or `{:error, reason}`
  when the target is ineligible or the request fails.
  """
  @spec transfer(non_neg_integer, node) :: :ok | :not_needed | {:error, term}
  def transfer(partition_idx, target_node) do
    if System.running?() do
      target = {Group.name(partition_idx), target_node}

      case Adapter.find_leader(Group.cluster_name(partition_idx)) do
        {:ok, leader} when leader == target ->
          :ok

        {:ok, leader} ->
          do_transfer(partition_idx, leader, target)

        {:error, _} = err ->
          report_failure(partition_idx, err)

        other ->
          report_failure(partition_idx, {:unexpected_find_leader, other})
      end
    else
      :not_needed
    end
  end

  defp do_transfer(partition_idx, leader, target) do
    with :ok <- voter?(leader, target),
         :ok <- caught_up?(leader, target),
         :ok <- request_transfer(leader, target) do
      Logger.info(
        "super_cache, raft.leadership, transferred partition #{partition_idx} " <>
          "to #{inspect(target)}"
      )

      :ok
    else
      {:error, reason} -> report_failure(partition_idx, reason)
    end
  end

  defp report_failure(partition_idx, reason) do
    Logger.warning(
      "super_cache, raft.leadership, transfer failed for partition #{partition_idx}: " <>
        inspect(reason)
    )

    {:error, reason}
  end

  defp voter?(leader, target) do
    case Adapter.member_overview(leader, @timeout) do
      %{cluster: cluster} when is_map(cluster) ->
        if voter?(Map.get(cluster, target, %{})) do
          :ok
        else
          {:error, :target_not_voter}
        end

      # Without role information the engine's own transfer checks apply.
      _ ->
        :ok
    end
  end

  defp voter?(%{voter_status: %{membership: :voter}}), do: true
  defp voter?(%{membership: :voter}), do: true
  defp voter?(_), do: false

  defp caught_up?(leader, target) do
    leader_index = commit_index(Adapter.key_metrics(leader, @timeout))
    target_index = commit_index(Adapter.key_metrics(target, @timeout))

    if is_integer(leader_index) and is_integer(target_index) and target_index < leader_index do
      {:error, :target_not_caught_up}
    else
      :ok
    end
  end

  defp commit_index(%{commit_index: index}) when is_integer(index), do: index
  defp commit_index(_), do: nil

  defp request_transfer(leader, target) do
    case Adapter.transfer_leadership(leader, target, @timeout) do
      :ok -> :ok
      {:ok, _, _} -> :ok
      other -> {:error, other}
    end
  end
end
