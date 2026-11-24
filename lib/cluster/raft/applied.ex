defmodule SuperCache.Cluster.Raft.Applied do
  @moduledoc """
  Tracks the highest Raft log index applied to each local ETS partition.

  The watermark makes state-machine application idempotent: an index that was
  already applied (for example during log replay after a restart) is
  acknowledged without touching the cached data again. This is what keeps
  non-idempotent operations such as `increment` correct across replay.
  """

  alias SuperCache.Cluster.Raft.Store

  @doc "Return the highest applied index for a partition (0 when unknown)."
  @spec get(non_neg_integer) :: non_neg_integer
  def get(partition_idx) do
    case :ets.lookup(Store.applied_table(), partition_idx) do
      [{^partition_idx, index}] when is_integer(index) -> index
      _ -> 0
    end
  end

  @doc """
  Claim an index for a partition, returning `true` when it is newer than the
  current watermark and should be applied, or `false` when it is a replay.
  """
  @spec claim(non_neg_integer, non_neg_integer) :: boolean
  def claim(partition_idx, index) when is_integer(index) and index >= 0 do
    if index > get(partition_idx) do
      :ets.insert(Store.applied_table(), {partition_idx, index})
      true
    else
      false
    end
  end

  @doc "Reset a partition's watermark (used by tests and teardown)."
  @spec reset(non_neg_integer) :: :ok
  def reset(partition_idx) do
    :ets.delete(Store.applied_table(), partition_idx)
    :ok
  end

  @doc """
  Clear every partition watermark.

  Called when the ETS partitions are dropped: the next Raft replay starts from
  an empty state machine, so every committed entry must be applied again.
  """
  @spec reset_all() :: :ok
  def reset_all do
    case :ets.whereis(Store.applied_table()) do
      :undefined -> :ok
      _ -> :ets.delete_all_objects(Store.applied_table())
    end

    :ok
  end
end
