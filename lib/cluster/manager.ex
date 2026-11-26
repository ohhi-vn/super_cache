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
  4. **Ownership reconciliation** — whenever membership changes the partition
     assignment, nodes that *gain* responsibility for partitions they did not
     hold before pull those partitions from their previous holders, so records
     follow the map instead of stranding on old owners.

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

  alias SuperCache.Cluster.{Metrics, Replicator}
  alias SuperCache.Cluster.Raft.{Applied, Group, Leadership, System, Topology}
  alias SuperCache.Cluster.Raft.Group.Reconciler
  alias SuperCache.{Config, Partition}

  @pt_key {__MODULE__, :partition_map}
  @transfers __MODULE__.Transfers

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
  def full_sync, do: GenServer.cast(__MODULE__, :full_sync)

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
  def live_nodes, do: GenServer.call(__MODULE__, :live_nodes)

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
  @spec replication_mode() :: :async | :sync | :strong | :raft
  def replication_mode do
    Config.get_config(:replication_mode, :async)
  end

  @doc """
  Return the configured replication factor (total copies, primary included).

  Zero-cost read from `SuperCache.Config` — no GenServer hop.
  Defaults to `2`.

  ## Example

      SuperCache.Cluster.Manager.replication_factor()
      # => 2
  """
  @spec replication_factor() :: pos_integer()
  def replication_factor do
    Config.get_config(:replication_factor, 2)
  end

  # ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    ensure_transfer_table()
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

    if Config.get_config(:replication_mode, :async) == :raft do
      Topology.refresh(nodes)
    end

    {:noreply, %{nodes: nodes}}
  end

  @impl true
  def handle_cast({:node_up, new_node}, %{nodes: nodes} = state) do
    cond do
      new_node == node() ->
        # Our own node is always considered live; no health-check needed.
        updated = Enum.uniq([new_node | nodes])
        update_partition_map(updated)
        schedule_raft_reconcile()
        {:noreply, %{state | nodes: updated}}

      node_running?(new_node) ->
        updated = Enum.uniq([new_node | nodes])
        update_partition_map(updated)
        schedule_raft_reconcile()
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
      update_partition_map(updated)
      schedule_raft_reconcile()
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
        update_partition_map(updated)
        schedule_raft_reconcile()
        spawn(fn -> sync_to_node(target) end)

        Logger.info(
          "super_cache, cluster_manager, node up (retry #{attempt}): #{inspect(target)}"
        )

        {:noreply, %{state | nodes: updated}}

      attempt < 30 ->
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

  # Publish a new partition map and reconcile data ownership.
  #
  # Whenever membership changes the partition assignment shifts: partitions
  # this node did not hold before may become its responsibility while the
  # records still live on the previous holders.  `sync_to_node/1` only pushes
  # data to *joining* nodes, so without reconciliation those records would
  # never reach nodes that gained ownership after a join or a leave.
  defp schedule_raft_reconcile do
    if Config.get_config(:replication_mode, :async) == :raft and System.running?() do
      spawn(fn -> reconcile_local_raft_groups() end)
    end
  end

  defp reconcile_local_raft_groups do
    Group.local_partitions()
    |> Enum.filter(fn idx ->
      {primary, _replicas} = get_replicas(idx)
      primary == node()
    end)
    |> Enum.each(&Reconciler.reconcile/1)
  end

  defp update_partition_map(nodes) do
    old_map = :persistent_term.get(@pt_key, %{})
    new_map = build_partition_map(nodes)
    :persistent_term.put(@pt_key, new_map)
    Metrics.increment(:membership, :changes)

    if Config.get_config(:replication_mode, :async) == :raft do
      Topology.refresh(nodes)
    end

    drop_stale_raft_groups(old_map, new_map)

    # Raft member preparation belongs to the Reconciler: it prepares a member
    # immediately before starting and joining it. Preparing here as well races
    # with that flow and can force-delete a server the Reconciler just started,
    # leaving the peer unable to catch up and permanently promotable.
    spawn(fn -> reconcile_ownership(old_map, new_map) end)

    :ok
  end

  # A node that was primary for a partition may have formed its Raft group
  # before the rest of the cluster was visible. If primacy later moves to
  # another node, that local group keeps electing itself with a stale member
  # set and competes with the new primary's group (same cluster/server name).
  # Stop it so only the current primary owns group formation.
  defp drop_stale_raft_groups(old_map, new_map) do
    if Config.get_config(:replication_mode, :async) == :raft and System.running?() do
      Enum.each(Group.tracked(), &stop_stale_group_if_needed(&1, old_map, new_map))
    end
  end

  defp stop_stale_group_if_needed(idx, old_map, new_map) do
    {old_primary, _old_replicas} = Map.get(old_map, idx, {node(), []})
    {new_primary, new_replicas} = Map.get(new_map, idx, {node(), []})

    cond do
      node() not in [new_primary | new_replicas] ->
        stop_stale_group(idx, :not_a_member)

      old_primary == node() and new_primary != node() ->
        transfer_leadership_if_planned(idx, new_primary)
        stop_stale_group(idx, :primary_changed)

      true ->
        :ok
    end
  end

  # A planned primary move lets the current leader hand over to the incoming
  # primary before this node tears down its group. Failures are counted and
  # deliberately ignored: the group is still stopped and the new primary's
  # reconciler falls back to the normal formation/election path.
  defp transfer_leadership_if_planned(idx, new_primary) do
    case Leadership.transfer(idx, new_primary) do
      :ok ->
        :ok

      :not_needed ->
        :ok

      {:error, reason} ->
        Metrics.increment(:replication, :raft_failures)
        Metrics.increment(:replication, :leadership_transfer_failures)

        Logger.warning(
          "super_cache, cluster_manager, leadership transfer for partition #{idx} " <>
            "to #{inspect(new_primary)} failed: #{inspect(reason)}; falling back to election"
        )

        :ok
    end
  end

  defp stop_stale_group(idx, reason) do
    Logger.info(
      "super_cache, cluster_manager, stopping local raft group for partition " <>
        "#{idx} (#{reason})"
    )

    Group.stop(idx)

    if reason == :not_a_member do
      case SuperCache.Partition.get_partition_by_idx(idx) do
        nil -> :ok
        partition -> SuperCache.Storage.delete_all(partition)
      end

      Applied.reset(idx)
    end
  end

  # Pull partitions this node newly holds from their previous holders.
  # Runs outside the GenServer so slow transfers never block membership
  # handling.  Best-effort: if no previous holder is reachable there is
  # nothing to pull from.
  defp reconcile_ownership(old_map, new_map) do
    me = node()
    live = live_set()

    Enum.each(new_map, fn {idx, {primary, replicas}} ->
      reconcile_partition(idx, primary, replicas, old_map, me, live)
    end)
  end

  defp reconcile_partition(idx, primary, replicas, old_map, me, live) do
    holders_now = [primary | replicas]
    {old_primary, old_replicas} = Map.get(old_map, idx, {nil, []})
    holders_before = Enum.uniq([old_primary | old_replicas])

    if me in holders_now and me not in holders_before do
      if Config.get_config(:replication_mode, :async) == :raft do
        # Raft members rebuild from the committed log; no ETS handoff.
        :ok
      else
        handle_donor(idx, select_donor(old_primary, old_replicas, me, live), me)
      end
    end
  end

  defp handle_donor(idx, nil, _me) do
    SuperCache.Log.debug(fn ->
      "super_cache, cluster_manager, no donor for partition #{idx} — " <>
        "no previous holder is connected"
    end)
  end

  defp handle_donor(_idx, me, me), do: :ok

  defp handle_donor(idx, donor, me) do
    Logger.info(
      "super_cache, cluster_manager, pulling partition #{idx} from " <>
        "#{inspect(donor)} (ownership moved to #{inspect(me)})"
    )

    request_partition(idx, donor)
  end

  defp request_partition(idx, donor) do
    case :erpc.call(donor, Replicator, :push_partition, [idx, node()], 15_000) do
      :ok ->
        Metrics.increment(:handoff, :successes)

      {:error, _} ->
        Metrics.increment(:handoff, :failures)
        Metrics.increment(:handoff, :transfer_failures)
    end
  catch
    kind, reason ->
      Logger.warning(
        "super_cache, cluster_manager, failed to pull partition #{idx} from " <>
          "#{inspect(donor)}: #{inspect({kind, reason})}"
      )

      Metrics.increment(:handoff, :failures)
      Metrics.increment(:handoff, :transfer_failures)
  end

  # Build %{partition_idx => {primary, [replicas]}}.
  # `nodes` is guaranteed non-empty (always contains at least node()).
  defp build_partition_map(nodes) when nodes != [] do
    factor = replication_factor()
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

  # Push all partitions this node is the deterministic donor for to `target`.
  defp sync_to_node(target) do
    if Config.get_config(:replication_mode, :async) == :raft do
      :ok
    else
      do_sync_to_node(target)
    end
  end

  defp do_sync_to_node(target) do
    num_parts = Config.get_config(:num_partition, Partition.get_schedulers())
    live = live_set()

    for idx <- 0..(num_parts - 1) do
      {primary, replicas} = get_replicas(idx)

      if select_donor(primary, replicas, target, live) == node() do
        transfer_partition(idx, target)
      end
    end

    :ok
  end

  # Deterministic single donor for a partition: the primary when alive and not
  # the target, otherwise the first live replica that is not the target.
  defp select_donor(primary, replicas, target, live) do
    donor_for(primary, replicas, target, live)
  end

  @doc false
  # Public for tests and diagnostics: return the deterministic donor for a
  # partition without performing a transfer.
  @spec donor_for(node | nil, [node], node, MapSet.t()) :: node | nil
  def donor_for(primary, replicas, target, live) do
    [primary | replicas]
    |> Enum.reject(&(is_nil(&1) or &1 == target))
    |> Enum.find(&MapSet.member?(live, &1))
  end

  # Coalesce duplicate concurrent transfers for the same {partition, target}.
  defp transfer_partition(idx, target) do
    ensure_transfer_table()

    if :ets.insert_new(@transfers, {{idx, target}, true}) do
      try do
        case Replicator.push_partition(idx, target) do
          :ok ->
            Metrics.increment(:handoff, :successes)

          {:error, _} ->
            Metrics.increment(:handoff, :failures)
            Metrics.increment(:handoff, :transfer_failures)
        end
      after
        :ets.delete(@transfers, {idx, target})
      end
    else
      SuperCache.Log.debug(fn ->
        "super_cache, cluster_manager, transfer #{idx} → #{inspect(target)} already in flight"
      end)

      :ok
    end
  end

  defp live_set do
    Node.list()
    |> MapSet.new()
    |> MapSet.put(node())
  end

  defp ensure_transfer_table do
    case :ets.info(@transfers) do
      :undefined ->
        :ets.new(@transfers, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  # Returns true when SuperCache.Cluster.Bootstrap is running on target_node.
  defp node_running?(target_node) do
    :erpc.call(target_node, SuperCache.Cluster.Bootstrap, :running?, [], 5_000)
  catch
    kind, reason ->
      Logger.warning(
        "super_cache, cluster_manager, health-check failed " <>
          "→ #{inspect(target_node)}: #{inspect({kind, reason})}"
      )

      false
  end

  # Guarantee node() is always in the membership list.
  defp ensure_self(nodes), do: Enum.uniq([node() | nodes])
end
