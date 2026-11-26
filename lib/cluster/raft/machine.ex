defmodule SuperCache.Cluster.Raft.Machine do
  @moduledoc """
  Raft state machine that applies committed commands to a local ETS partition.

  The machine is configured with the partition index it owns:

      {:machine, SuperCache.Cluster.Raft.Machine, %{partition_idx: 2}}

  Commands are either a single operation tuple (`{op_name, op_arg}`) or a
  batch (`{:batch, [op]}`). Both use the same local apply helpers as
  `SuperCache.Cluster.Replicator`, so Raft mode and the other replication
  modes write identical records to ETS.

  Application is guarded by the Raft log index stored in
  `SuperCache.Cluster.Raft.Applied`; a replayed index is acknowledged without
  re-applying its operations. The machine state also carries the highest
  applied index so snapshots checkpoint it as metadata, and installing a
  snapshot restores the ETS watermark.
  """

  @behaviour RaftEx.Domain.Machine

  alias SuperCache.Cluster.Raft.Applied
  alias SuperCache.Cluster.Replicator
  alias SuperCache.{Partition, Storage}

  @impl RaftEx.Domain.Machine
  def init(conf) do
    %{
      partition_idx: Map.fetch!(conf, :partition_idx),
      applied: Map.get(conf, :applied, 0)
    }
  end

  @impl RaftEx.Domain.Machine
  def apply(meta, command, %{partition_idx: partition_idx} = state) do
    index = Map.get(meta, :index, 0)

    if Applied.applied?(partition_idx, index) do
      {%{state | applied: max(index, Map.get(state, :applied, 0))}, {:ok, :already_applied}}
    else
      result =
        try do
          apply_command(command, partition_idx)
        rescue
          err -> {:error, {:apply_failed, err}}
        end

      case result do
        {:error, _} = err ->
          {state, err}

        _ ->
          Applied.record(partition_idx, index)
          {%{state | applied: index}, result}
      end
    end
  end

  @impl RaftEx.Domain.Machine
  def snapshot_installed(meta, state, _old_meta, _old_state) do
    index = Map.get(meta, :index, Map.get(state, :applied, 0))
    Applied.record(Map.fetch!(state, :partition_idx), index)
    []
  end

  defp apply_command({:batch, ops}, partition_idx) when is_list(ops) do
    results = Enum.map(ops, fn {op, arg} -> apply_op(op, arg, partition_idx) end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, :batch_apply_failed}
    end
  end

  defp apply_command({op, arg}, partition_idx), do: apply_op(op, arg, partition_idx)

  defp apply_op(:increment, {key, default, step}, partition_idx) do
    partition = Partition.get_partition_by_idx(partition_idx)
    Storage.update_counter(key, partition, {2, step}, {key, default})
    :ok
  end

  defp apply_op(op, arg, partition_idx) do
    Replicator.apply_op(partition_idx, op, arg)
  rescue
    err -> {:error, {:apply_failed, err}}
  end
end
