defmodule SuperCache.Cluster.Replicator do
  @moduledoc """
  Applies replicated writes on the local node and handles bulk partition
  transfers when a new node joins or a full sync is requested.

  Every call to `replicate/3` increments the appropriate
  `replicate_<mode>` counter in `SuperCache.Cluster.Metrics` and records
  a latency sample so the results are visible in
  `SuperCache.Cluster.Stats.api/0`.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, ThreePhaseCommit, Metrics}

  @batch_size 500

  # ── Public API ───────────────────────────────────────────────────────────────

  @spec replicate(non_neg_integer, atom, any) :: :ok | {:error, term}
  def replicate(partition_idx, op_name, op_arg \\ nil) do
    mode = SuperCache.Config.get_config(:replication_mode, :async)
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    if replicas == [] do
      :ok
    else
      metric_key = :"replicate_#{mode}"
      t0         = System.monotonic_time(:microsecond)

      result =
        try do
          do_replicate(mode, partition_idx, op_name, op_arg, replicas)
        rescue
          err -> {:error, err}
        end

      elapsed = System.monotonic_time(:microsecond) - t0
      Metrics.increment({:api, metric_key}, :calls)
      Metrics.push_latency({:api_latency_us, metric_key}, elapsed)

      case result do
        {:error, _} -> Metrics.increment({:api, metric_key}, :errors)
        _           -> :ok
      end

      result
    end
  end

  @doc """
  Apply a replicated operation on this node.

  Called via `:erpc` from the primary — **do NOT call directly**.
  """
  @spec apply_op(non_neg_integer, atom, any) :: :ok
  def apply_op(partition_idx, op_name, op_arg) do
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

  @doc """
  Push all records for `partition_idx` to `target_node` in batches.
  """
  @spec push_partition(non_neg_integer, node) :: :ok
  def push_partition(partition_idx, target_node) do
    partition = Partition.get_partition_by_idx(partition_idx)

    Logger.info(
      "super_cache, replicator, syncing partition #{partition_idx} → #{inspect(target_node)}"
    )

    records = Storage.scan(fn rec, acc -> [rec | acc] end, [], partition)
    total   = length(records)

    records
    |> Enum.chunk_every(@batch_size)
    |> Enum.with_index(1)
    |> Enum.each(fn {batch, batch_num} ->
      try do
        :erpc.call(target_node, __MODULE__, :apply_op,
                   [partition_idx, :put, batch], 10_000)

        SuperCache.Log.debug(fn ->
          "super_cache, replicator, partition #{partition_idx} batch #{batch_num} " <>
          "(#{length(batch)} records) → #{inspect(target_node)}"
        end)
      catch
        kind, reason ->
          Logger.warning(
            "super_cache, replicator, batch #{batch_num} failed " <>
            "→ #{inspect(target_node)}: #{inspect({kind, reason})}"
          )
      end
    end)

    Logger.info(
      "super_cache, replicator, pushed #{total} record(s) for partition " <>
      "#{partition_idx} → #{inspect(target_node)}"
    )

    :ok
  end

  # ── Private ──────────────────────────────────────────────────────────────────

  defp do_replicate(:async, partition_idx, op_name, op_arg, replicas) do
    spawn(fn ->
      Enum.each(replicas, &apply_remote(&1, partition_idx, op_name, op_arg))
    end)
    :ok
  end

  defp do_replicate(:sync, partition_idx, op_name, op_arg, replicas) do
    Enum.each(replicas, &apply_remote(&1, partition_idx, op_name, op_arg))
    :ok
  end

  defp do_replicate(:strong, partition_idx, op_name, op_arg, _replicas) do
    ThreePhaseCommit.commit(partition_idx, [{op_name, op_arg}])
  end

  defp apply_remote(target_node, partition_idx, op_name, op_arg) do
    try do
      :erpc.call(target_node, __MODULE__, :apply_op,
                 [partition_idx, op_name, op_arg], 5_000)
    catch
      kind, reason ->
        Logger.warning(
          "super_cache, replicator, failed → #{inspect(target_node)}: " <>
          "#{inspect({kind, reason})}"
        )
    end
  end
end
