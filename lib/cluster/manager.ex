defmodule SuperCache.Cluster.Manager do
  @moduledoc """
  Maintains the cluster membership list and the partition → primary/replica
  mapping.

  ## Responsibilities

  1. **Membership tracking** — reacts to `:nodeup` / `:nodedown` events
     forwarded by `SuperCache.Cluster.NodeMonitor`.
  2. **Partition map** — builds and republishes a
     `%{partition_idx => {primary, [replicas]}}` map whenever membership
     changes.  The map is stored in `:persistent_term` so hot-path reads
     are allocation-free (no GenServer hop).
  3. **Full sync** — when a new node joins, triggers
     `Replicator.push_partition/2` for every partition that this node owns
     (as primary or replica) so the joining node receives a consistent
     snapshot.

  ## Cold-start behaviour

  During application boot, `SuperCache.Cluster.Bootstrap.start!/1` has not
  been called yet, so `node_running?/1` returns `false` even for `node()`
  itself.  The manager therefore always seeds the membership list with
  `node()` regardless of the health-check result, so the partition map is
  never built from an empty list.  Remote peers are added only when the
  health-check confirms they are running.

  ## Adding nodes at runtime

  Nodes are added automatically via `:nodeup` events delivered by
  `SuperCache.Cluster.NodeMonitor`.  You can also add a node manually:

      SuperCache.Cluster.Manager.node_up(:peer@host)

  ## Partition assignment

  Partitions are assigned by rotating the sorted node list.  With `N` nodes
  and replication factor `R`, partition `idx` gets:

  - **primary** — `sorted_nodes[idx mod N]`
  - **replicas** — the next `min(R-1, N-1)` nodes in the rotated list

  This gives a balanced, deterministic assignment with no external
  coordination.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger
  require SuperCache.Log

  alias SuperCache.{Config, Partition}

  @pt_key {__MODULE__, :partition_map}

  # ── Public API ───────────────────────────────────────────────────────────────

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Notify the manager that `node` has connected."
  @spec node_up(node) :: :ok
  def node_up(node), do: GenServer.cast(__MODULE__, {:node_up, node})

  @doc "Notify the manager that `node` has disconnected."
  @spec node_down(node) :: :ok
  def node_down(node), do: GenServer.cast(__MODULE__, {:node_down, node})

  @doc "Trigger a full partition sync from this node to all peers."
  @spec full_sync() :: :ok
  def full_sync(), do: GenServer.cast(__MODULE__, :full_sync)

  @doc """
  Return `{primary_node, [replica_nodes]}` for `partition_idx`.

  Zero-cost read from `:persistent_term` — no GenServer hop.
  Falls back to `{node(), []}` when the map has not been built yet.
  """
  @spec get_replicas(non_neg_integer) :: {node, [node]}
  def get_replicas(partition_idx) do
    :persistent_term.get(@pt_key, %{})
    |> Map.get(partition_idx, {node(), []})
  end

  @doc "Return the current list of live nodes (includes this node)."
  @spec live_nodes() :: [node]
  def live_nodes(), do: GenServer.call(__MODULE__, :live_nodes)

  @doc """
  Return the cluster-wide replication mode configured via
  `SuperCache.Cluster.Bootstrap.start!/1`.

  | Value     | Guarantee              |
  |-----------|------------------------|
  | `:async`  | Eventual (default)     |
  | `:sync`   | At-least-once delivery |
  | `:strong` | Three-phase commit     |

  Zero-cost read from `SuperCache.Config` — no GenServer hop.

  ## Example

      SuperCache.Cluster.Manager.replication_mode()
      # => :async
  """
  @spec replication_mode() :: :async | :sync | :strong
  def replication_mode() do
    Config.get_config(:replication_mode, :async)
  end

  # ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    {:ok, %{nodes: []}, {:continue, :init}}
  end

  @impl true
  def handle_continue(:init, _state) do
    # Always include node() itself so the partition map is never built from
    # an empty list.  Bootstrap has not run yet so node_running?(node())
    # returns false during a cold start — we skip the health check for self.
    remote_live =
      Node.list()
      |> Enum.filter(&node_running?/1)

    nodes = Enum.uniq([node() | remote_live])

    :persistent_term.put(@pt_key, build_partition_map(nodes))
    Logger.info("super_cache, cluster_manager, initial nodes: #{inspect(nodes)}")
    {:noreply, %{nodes: nodes}}
  end

  @impl true
  def handle_cast({:node_up, new_node}, %{nodes: nodes} = state) do
    cond do
      new_node == node() ->
        # Our own node is always considered live; no health-check needed.
        updated = Enum.uniq([new_node | nodes])
        :persistent_term.put(@pt_key, build_partition_map(updated))
        {:noreply, %{state | nodes: updated}}

      node_running?(new_node) ->
        updated = Enum.uniq([new_node | nodes])
        :persistent_term.put(@pt_key, build_partition_map(updated))
        spawn(fn -> sync_to_node(new_node) end)
        Logger.info("super_cache, cluster_manager, node up: #{inspect(new_node)}")
        {:noreply, %{state | nodes: updated}}

      true ->
        # Remote node connected but SuperCache not yet started on it.
        # Schedule a retry so we pick it up once Bootstrap completes there.
        SuperCache.Log.debug(fn ->
          "super_cache, cluster_manager, #{inspect(new_node)} connected but not ready — will retry"
        end)

        Process.send_after(self(), {:retry_node_up, new_node, 1}, 500)
        {:noreply, state}
    end
  end

  def handle_cast({:node_down, dead_node}, %{nodes: nodes} = state) do
    if dead_node in nodes do
      # Always keep node() in the list even if it receives its own nodedown
      # (should not happen, but defensive).
      updated = nodes |> List.delete(dead_node) |> ensure_self()
      :persistent_term.put(@pt_key, build_partition_map(updated))
      Logger.warning("super_cache, cluster_manager, node down: #{inspect(dead_node)}")
      {:noreply, %{state | nodes: updated}}
    else
      {:noreply, state}
    end
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

  @impl true
  def handle_info({:retry_node_up, target, attempt}, %{nodes: nodes} = state) do
    cond do
      target in nodes ->
        # Already added by a concurrent nodeup — nothing to do.
        {:noreply, state}

      node_running?(target) ->
        updated = Enum.uniq([target | nodes])
        :persistent_term.put(@pt_key, build_partition_map(updated))
        spawn(fn -> sync_to_node(target) end)

        Logger.info(
          "super_cache, cluster_manager, node up (retry #{attempt}): #{inspect(target)}"
        )

        {:noreply, %{state | nodes: updated}}

      attempt < 10 ->
        # Back-off: 500 ms * attempt (capped at 5 s).
        delay = min(500 * attempt, 5_000)
        Process.send_after(self(), {:retry_node_up, target, attempt + 1}, delay)

        SuperCache.Log.debug(fn ->
          "super_cache, cluster_manager, #{inspect(target)} still not ready " <>
            "(attempt #{attempt}) — retrying in #{delay} ms"
        end)

        {:noreply, state}

      true ->
        Logger.warning(
          "super_cache, cluster_manager, giving up on #{inspect(target)} after #{attempt} attempts"
        )

        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Private ──────────────────────────────────────────────────────────────────

  # Build %{partition_idx => {primary, [replicas]}}.
  # `nodes` is guaranteed non-empty (always contains at least node()).
  defp build_partition_map(nodes) when nodes != [] do
    factor = Config.get_config(:replication_factor, 2)
    num_parts = Config.get_config(:num_partition, Partition.get_schedulers())
    sorted = Enum.sort(nodes)
    node_count = length(sorted)

    for idx <- 0..(num_parts - 1), into: %{} do
      rotated = rotate(sorted, rem(idx, node_count))
      primary = hd(rotated)
      replicas = rotated |> tl() |> Enum.take(min(factor - 1, node_count - 1))
      {idx, {primary, replicas}}
    end
  end

  defp rotate(list, 0), do: list
  defp rotate([h | t], n), do: rotate(t ++ [h], n - 1)

  # Push all partitions this node owns to `target`.
  defp sync_to_node(target) do
    num_parts = Config.get_config(:num_partition, Partition.get_schedulers())
    me = node()

    for idx <- 0..(num_parts - 1) do
      {primary, replicas} = get_replicas(idx)

      if me == primary or me in replicas do
        SuperCache.Cluster.Replicator.push_partition(idx, target)
      end
    end
  end

  # Returns true when SuperCache.Cluster.Bootstrap is running on target_node.
  defp node_running?(target_node) do
    try do
      :erpc.call(target_node, SuperCache.Cluster.Bootstrap, :running?, [], 5_000)
    catch
      kind, reason ->
        Logger.warning(
          "super_cache, cluster_manager, health-check failed " <>
            "→ #{inspect(target_node)}: #{inspect({kind, reason})}"
        )

        false
    end
  end

  # Guarantee node() is always in the membership list.
  defp ensure_self(nodes), do: Enum.uniq([node() | nodes])
end
