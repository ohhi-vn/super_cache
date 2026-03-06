defmodule SuperCache.Cluster.Replicator do
  @moduledoc """
  Applies replicated writes on the local node and handles bulk partition
  transfers when a new node joins or a full sync is requested.

  Receives a `partition_idx` integer and resolves the ETS table via
  `Partition.get_partition_by_idx/1` — never via `get_partition/1` which
  would re-hash the index and produce a wrong table.
  """
  require Logger

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.Manager

  @batch_size 500

  @spec replicate(non_neg_integer, atom, any) :: :ok
  def replicate(partition_idx, op_name, op_arg \\ nil) do
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    unless replicas == [] do
      spawn(fn ->
        Enum.each(replicas, &apply_remote(&1, partition_idx, op_name, op_arg))
      end)
    end

    :ok
  end

  @doc """
  Apply a replicated operation on this node.
  Called via `:erpc` from the primary — do NOT call directly.
  """
  @spec apply_op(non_neg_integer, atom, any) :: :ok
  def apply_op(partition_idx, op_name, op_arg) do
    # Use get_partition_by_idx — the index must NOT be re-hashed.
    partition = Partition.get_partition_by_idx(partition_idx)

    case {op_name, op_arg} do
      {:put, records} when is_list(records) -> Storage.put(records, partition)
      {:put, record}                        -> Storage.put(record, partition)
      {:delete, key}                        -> Storage.delete(key, partition)
      {:delete_match, pattern}              -> Storage.delete_match(pattern, partition)
      {:delete_all, _}                      -> Storage.delete_all(partition)
    end

    :ok
  end

  @spec push_partition(non_neg_integer, node) :: :ok
  def push_partition(partition_idx, target_node) do
    # Use get_partition_by_idx — same reason as apply_op.
    partition = Partition.get_partition_by_idx(partition_idx)
    Logger.info("super_cache, replicator, syncing partition #{partition_idx} → #{inspect(target_node)}")

    records = Storage.scan(fn rec, acc -> [rec | acc] end, [], partition)

    records
    |> Enum.chunk_every(@batch_size)
    |> Enum.each(fn batch ->
      try do
        :erpc.call(target_node, __MODULE__, :apply_op, [partition_idx, :put, batch], 10_000)
      catch
        kind, reason ->
          Logger.warning("super_cache, replicator, batch failed → #{inspect(target_node)}: #{inspect({kind, reason})}")
      end
    end)

    Logger.info("super_cache, replicator, pushed #{length(records)} records for partition #{partition_idx}")
    :ok
  end

  defp apply_remote(target_node, partition_idx, op_name, op_arg) do
    try do
      :erpc.call(target_node, __MODULE__, :apply_op, [partition_idx, op_name, op_arg], 5_000)
    catch
      kind, reason ->
        Logger.warning("super_cache, replicator, failed → #{inspect(target_node)}: #{inspect({kind, reason})}")
    end
  end
end
