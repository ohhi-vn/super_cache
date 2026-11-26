defmodule SuperCache.Cluster.Raft.Topology do
  @moduledoc """
  Derives the Raft cluster sizing diagnostic.

  A cluster with `replication_factor: N` is considered healthy when it has at
  least `N + 1` live nodes, so a spare node remains available for failover
  and repair. Below that threshold the topology is `:underprovisioned`; the
  status is advisory and never blocks startup or writes.
  """

  require Logger

  alias SuperCache.Cluster.Manager

  @pt_key {__MODULE__, :status}

  @doc "Evaluate topology for a given node count and replication factor."
  @spec evaluate(non_neg_integer, non_neg_integer) :: map
  def evaluate(node_count, replication_factor) do
    recommended = replication_factor + 1

    status =
      if node_count >= recommended do
        :ok
      else
        :underprovisioned
      end

    %{
      node_count: node_count,
      replication_factor: replication_factor,
      recommended_nodes: recommended,
      status: status,
      evaluated_at: System.monotonic_time(:millisecond)
    }
  end

  @doc "Refresh the stored topology from the current cluster state."
  @spec refresh() :: map
  def refresh, do: refresh(Manager.live_nodes())

  @doc "Refresh the stored topology from an explicit node list."
  @spec refresh([node]) :: map
  def refresh(nodes) when is_list(nodes) do
    if Manager.replication_mode() != :raft do
      :persistent_term.erase(@pt_key)
      nil
    else
      do_refresh(nodes)
    end
  end

  defp do_refresh(nodes) do
    node_count = length(nodes)
    rf = Manager.replication_factor()
    new_status = evaluate(node_count, rf)

    previous = :persistent_term.get(@pt_key, nil)

    if previous == nil or previous.status != new_status.status do
      log_transition(new_status, node_count, rf)
    end

    :persistent_term.put(@pt_key, new_status)
    new_status
  end

  defp log_transition(%{status: :underprovisioned} = s, node_count, rf) do
    Logger.warning(
      "super_cache, raft.topology, under-provisioned: #{node_count} live node(s), " <>
        "recommended #{s.recommended_nodes} for replication_factor " <>
        "#{rf}; startup and writes continue"
    )
  end

  defp log_transition(s, node_count, _rf) do
    Logger.info(
      "super_cache, raft.topology, healthy: #{node_count} live node(s) " <>
        "(recommended #{s.recommended_nodes})"
    )
  end

  @doc "Return the last evaluated topology, or nil when raft mode is inactive."
  @spec status() :: map | nil
  def status do
    :persistent_term.get(@pt_key, nil)
  end
end
