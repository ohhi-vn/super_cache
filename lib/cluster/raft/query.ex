defmodule SuperCache.Cluster.Raft.Query do
  @moduledoc """
  Raft-native query barriers for reads of Raft-managed partitions.

  SuperCache keeps cache records in local ETS tables, so a query cannot return
  the data itself. Instead the Raft query path establishes the required
  ordering guarantee (local state, leader, or a voter quorum) before the
  caller reads the ETS partition.
  """

  alias SuperCache.Cluster.Raft.{Adapter, Config, Group, System}

  @doc false
  @spec confirm(term) :: {:ok, term}
  def confirm(state), do: {:ok, state}

  @doc """
  Establish the query guarantee for `mode` on a Raft-managed partition.

  Returns `:ok` when Raft mode is inactive or the barrier succeeded, otherwise
  `{:error, reason}` for the caller to normalize.
  """
  @spec barrier(atom, non_neg_integer, timeout() | nil) :: :ok | {:error, term}
  def barrier(mode, partition_idx, timeout \\ nil)

  def barrier(mode, partition_idx, nil),
    do: barrier(mode, partition_idx, Config.get(:command_timeout_ms))

  def barrier(mode, partition_idx, timeout) do
    if System.running?() do
      query_result(mode, partition_idx, timeout)
    else
      :ok
    end
  end

  defp query_result(mode, partition_idx, timeout) do
    server = {Group.name(partition_idx), node()}
    result = run_query(mode, server, {__MODULE__, :confirm, []}, timeout)
    normalize(result)
  end

  defp run_query(mode, server, query, timeout) do
    case mode do
      :local -> Adapter.local_query(server, query, timeout)
      :primary -> Adapter.leader_query(server, query, timeout)
      :consistent -> Adapter.consistent_query(server, query, timeout)
    end
  end

  defp normalize(result) do
    case result do
      {:ok, _, _} -> :ok
      :ok -> :ok
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
      other -> {:error, other}
    end
  end
end
