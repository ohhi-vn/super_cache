defmodule SuperCache.Cluster.Manager do
  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger

  alias SuperCache.{Config, Partition}

  @pt_key {__MODULE__, :partition_map}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  def node_up(node),    do: GenServer.cast(__MODULE__, {:node_up, node})
  def node_down(node),  do: GenServer.cast(__MODULE__, {:node_down, node})
  def full_sync(),      do: GenServer.cast(__MODULE__, :full_sync)

  @doc """
  Return `{primary_node, [replica_nodes]}` for a partition index.
  Zero-cost read from `:persistent_term`.
  """
  @spec get_replicas(non_neg_integer) :: {node, [node]}
  def get_replicas(partition_idx) do
    :persistent_term.get(@pt_key, %{})
    |> Map.get(partition_idx, {node(), []})
  end

  @spec live_nodes() :: [node]
  def live_nodes(), do: GenServer.call(__MODULE__, :live_nodes)

  @impl true
  def init(_opts) do
    nodes = [node() | Node.list()]
    :persistent_term.put(@pt_key, build_partition_map(nodes))
    Logger.info("super_cache, cluster_manager, nodes: #{inspect(nodes)}")
    {:ok, %{nodes: nodes}}
  end

  @impl true
  def handle_cast({:node_up, new_node}, %{nodes: nodes} = state) do
    updated = Enum.uniq([new_node | nodes])
    :persistent_term.put(@pt_key, build_partition_map(updated))
    spawn(fn -> sync_to_node(new_node) end)
    Logger.info("super_cache, cluster_manager, node up: #{inspect(new_node)}")
    {:noreply, %{state | nodes: updated}}
  end

  def handle_cast({:node_down, dead_node}, %{nodes: nodes} = state) do
    updated = List.delete(nodes, dead_node)
    :persistent_term.put(@pt_key, build_partition_map(updated))
    Logger.warning("super_cache, cluster_manager, node down: #{inspect(dead_node)}")
    {:noreply, %{state | nodes: updated}}
  end

  def handle_cast(:full_sync, %{nodes: nodes} = state) do
    nodes
    |> List.delete(node())
    |> Enum.each(&spawn(fn -> sync_to_node(&1) end))
    {:noreply, state}
  end

  @impl true
  def handle_call(:live_nodes, _from, %{nodes: nodes} = state) do
    {:reply, nodes, state}
  end

  ## Private ─────────────────────────────────────────────────────────────────

  defp build_partition_map(nodes) do
    factor     = Config.get_config(:replication_factor, 2)
    num_parts  = Config.get_config(:num_partition, Partition.get_schedulers())
    sorted     = Enum.sort(nodes)
    node_count = length(sorted)

    for idx <- 0..(num_parts - 1), into: %{} do
      rotated  = rotate(sorted, idx)
      primary  = hd(rotated)
      replicas = rotated |> tl() |> Enum.take(min(factor - 1, node_count - 1))
      {idx, {primary, replicas}}
    end
  end

  defp rotate(list, 0), do: list
  defp rotate([h | t], n), do: rotate(t ++ [h], n - 1)

  defp sync_to_node(target) do
    num_parts = Config.get_config(:num_partition, Partition.get_schedulers())
    me        = node()

    for idx <- 0..(num_parts - 1) do
      {primary, replicas} = get_replicas(idx)
      # Push if we are the primary OR a replica for this partition.
      # This covers the case where the rejoining node was previously primary,
      # died, and the partition was taken over by a replica — after rejoin the
      # partition map may reassign the partition back to the rejoining node,
      # so only the former replica (now acting primary) has the live data.
      if me == primary or me in replicas do
        SuperCache.Cluster.Replicator.push_partition(idx, target)
      end
    end
  end
end
