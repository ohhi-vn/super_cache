defmodule SuperCache.Cluster.Raft.Group.Reconciler do
  @moduledoc """
  Serializes Raft membership changes and limits them to the partition's
  primary node, so concurrent cluster events cannot issue overlapping
  join/leave operations for the same Raft group.

  Membership is reconciled in two ways:

    * when the group has a leader, missing members are started and joined and
      stale members are removed;
    * when the group has no leader (first formation, or a member departure
      that took the leader with it), the primary reforms the cluster from
      the current partition map so the group converges; survivors recover
      their persisted logs and the new member catches up from the group.
      Reformation after a member departure is a pure-removal change: no
      committed entry is deleted to make it happen.

  A join that cannot converge is different: when a member fails to catch up
  (`:catch_up_timeout`/`:not_caught_up`) while the group holds committed
  data, reconciliation reports a failure instead of rebuilding the group
  from scratch with an empty log, so committed entries are never discarded
  by a catch-up repair.

  Starting a member that still runs a stale server (for example a self-only
  group formed before the rest of the cluster was visible) first stops that
  server, so the new member always starts from the current membership.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger

  alias SuperCache.Cluster.Manager
  alias SuperCache.Cluster.Raft.{Adapter, Group, System}

  # Membership changes involve a log append, replication and a commit; they
  # are slower than a normal command, so they get their own budget instead of
  # the short `:command_timeout_ms` used for writes.
  @membership_timeout 10_000

  # When no leader is usable, an election may still be in progress. Wait this
  # long for one to emerge before reforming, so a transient election is not
  # destroyed by reformation. A genuine departure never elects, and the wait
  # then expires into reformation.
  @leader_wait_ms 5_000
  @leader_poll_ms 100

  # Promotion can be rejected while a freshly started member replays the
  # leader's log. Budget enough time for that catch-up without letting a failed
  # reconcile leave the member permanently promotable.
  @promotion_timeout 15_000

  @doc "Start the reconciler (owned by the application supervision tree)."
  @spec start_link(keyword) :: {:ok, pid} | {:error, term}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Reconcile one partition's Raft group with the current partition map."
  @spec reconcile(non_neg_integer) :: :ok | {:error, term}
  def reconcile(partition_idx) do
    GenServer.call(__MODULE__, {:reconcile, partition_idx}, 30_000)
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:reconcile, partition_idx}, _from, state) do
    {:reply, do_reconcile(partition_idx), state}
  end

  defp do_reconcile(partition_idx) do
    {primary, _replicas} = Manager.get_replicas(partition_idx)

    # Only the partition's primary owns Raft membership for its group.
    if primary != node() do
      :ok
    else
      reconcile_primary(partition_idx)
    end
  rescue
    err ->
      Logger.warning("super_cache, raft.group, reconcile failed: #{inspect(err)}")
      {:error, err}
  end

  defp reconcile_primary(partition_idx) do
    desired = Group.member_ids(partition_idx)
    cluster_name = Group.cluster_name(partition_idx)

    case Adapter.find_leader(cluster_name) do
      {:ok, leader} ->
        handle_leader_reconcile(leader, desired, cluster_name, partition_idx)

      {:error, _} ->
        # No leader: an election may be in progress — wait for it before
        # reforming, so a transient election is not destroyed. A genuine
        # departure never produces a leader, and reformation then converges
        # the group from the current partition map.
        reform_unless_leader_appears(desired, cluster_name, partition_idx)
    end
  end

  defp handle_leader_reconcile(leader, desired, cluster_name, partition_idx) do
    case reconcile_with_leader(leader, desired, cluster_name, partition_idx) do
      {:error, :noproc} ->
        # The leader process is gone; survivors may still elect a new one.
        reform_unless_leader_appears(desired, cluster_name, partition_idx)

      {:error, reason} when reason in [:catch_up_timeout, :not_caught_up] ->
        handle_catch_up_failure(reason, desired, cluster_name, partition_idx)

      other ->
        other
    end
  end

  # Wait briefly for an in-progress election before reforming. Reforming
  # while an election can still succeed destroys the emerging leader and
  # restarts the group from scratch; waiting preserves it. The follow-up
  # in-place attempt never loops back into waiting, so reconciliation
  # always terminates.
  defp reform_unless_leader_appears(desired, cluster_name, partition_idx) do
    case await_leader(cluster_name, @leader_wait_ms) do
      {:ok, leader} ->
        settle_with_leader(leader, desired, cluster_name, partition_idx)

      {:error, _} ->
        form_cluster(desired, cluster_name, partition_idx)
    end
  end

  # Single in-place attempt after the wait; falls back to reformation
  # without waiting again.
  defp settle_with_leader(leader, desired, cluster_name, partition_idx) do
    case reconcile_with_leader(leader, desired, cluster_name, partition_idx) do
      {:error, :noproc} ->
        form_cluster(desired, cluster_name, partition_idx)

      {:error, reason} when reason in [:catch_up_timeout, :not_caught_up] ->
        handle_catch_up_failure(reason, desired, cluster_name, partition_idx)

      other ->
        other
    end
  end

  defp await_leader(cluster_name, remaining) when remaining <= 0 do
    Adapter.find_leader(cluster_name)
  end

  defp await_leader(cluster_name, remaining) do
    case Adapter.find_leader(cluster_name) do
      {:ok, _} = ok ->
        ok

      {:error, _} ->
        Process.sleep(@leader_poll_ms)
        await_leader(cluster_name, remaining - @leader_poll_ms)
    end
  end

  # An incremental join can leave a peer stuck as promotable (its match_index
  # never advances). Rebuilding from scratch here would discard committed log
  # entries, so refuse when committed data exists and report the failure.
  defp handle_catch_up_failure(reason, desired, cluster_name, partition_idx) do
    Logger.warning(
      "super_cache, raft.group, join failed (#{inspect(reason)}) — " <>
        "refusing destructive reformation when committed data exists"
    )

    safe_form_cluster(desired, cluster_name, partition_idx)
  end

  defp reconcile_with_leader(leader, desired, cluster_name, partition_idx) do
    case Adapter.members(leader, @membership_timeout) do
      {:ok, current, _leader} ->
        {to_add, to_remove} = Group.diff_members(current, desired)

        Enum.each(to_remove, &leave(&1, leader))

        case join_all(to_add, leader, desired, cluster_name, partition_idx) do
          :ok ->
            case await_members(leader, desired, @membership_timeout) do
              :ok ->
                # Only stop removed servers once their removal is committed.
                Enum.each(to_remove, &stop_server/1)
                :ok

              {:error, _} = err ->
                err
            end

          {:error, _} = err ->
            err
        end

      {:error, _} = err ->
        err
    end
  end

  defp join_all([], _leader, _desired, _cluster_name, _partition_idx), do: :ok

  defp join_all([server_id | rest], leader, desired, cluster_name, partition_idx) do
    case join(server_id, leader, desired, cluster_name, partition_idx) do
      :ok ->
        join_all(rest, leader, desired, cluster_name, partition_idx)

      {:error, reason} when reason in [:catch_up_timeout, :not_caught_up] ->
        {:error, reason}

      # Other join failures are reported and skipped, matching the previous
      # best-effort behaviour of Enum.each.
      {:error, _} ->
        join_all(rest, leader, desired, cluster_name, partition_idx)
    end
  end

  defp join(server_id, leader, desired, cluster_name, partition_idx) do
    {_name, target_node} = server_id
    name = Group.name(partition_idx)

    with :ok <- Group.prepare_member(partition_idx, target_node),
         :ok <- start_server(server_id, desired, cluster_name, partition_idx) do
      wait_for_server(target_node, name, 5_000)
      do_add_member(leader, server_id)
    end
  end

  defp do_add_member(leader, server_id) do
    deadline = :erlang.monotonic_time(:millisecond) + @promotion_timeout

    with :ok <- ensure_promotable(leader, server_id),
         :ok <- await_catch_up(leader, server_id, deadline),
         :ok <- promote_when_caught_up(leader, server_id, deadline) do
      :ok
    else
      other ->
        Logger.warning("super_cache, raft.group, add_member failed: #{inspect(other)}")
        other
    end
  end

  # A previous reconcile may already have added the server as a promotable
  # member without promoting it, so re-adding is not required to retry.
  defp ensure_promotable(leader, server_id) do
    case Adapter.add_member(leader, server_id, :promotable, @membership_timeout) do
      {:ok, _, _} -> :ok
      {:error, :already_member} -> :ok
      {:error, _} = error -> error
    end
  end

  # raft_ex rejects promotion while the peer's match_index trails the leader's
  # commit_index. The metrics pre-check can return early when either side does
  # not expose an index, so retry :not_caught_up until the deadline instead of
  # leaving the member permanently promotable.
  defp promote_when_caught_up(leader, server_id, deadline) do
    remaining = deadline - :erlang.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, :catch_up_timeout}
    else
      case Adapter.promote_member(leader, server_id, min(remaining, @membership_timeout)) do
        {:ok, _, _} ->
          :ok

        {:error, :already_voter} ->
          :ok

        {:error, :not_caught_up} ->
          case await_catch_up(leader, server_id, deadline) do
            :ok ->
              Process.sleep(100)
              promote_when_caught_up(leader, server_id, deadline)

            {:error, _} = error ->
              error
          end

        {:error, _} = error ->
          error
      end
    end
  end

  # Promotion is safe only once the joining server has caught up with the
  # leader's committed log. When either side does not expose metrics we let the
  # engine's own catch-up check decide, rather than blocking a valid join.
  defp await_catch_up(leader, server_id, deadline) do
    if :erlang.monotonic_time(:millisecond) >= deadline do
      {:error, :catch_up_timeout}
    else
      leader_index = commit_index(Adapter.key_metrics(leader, 2_000))
      member_index = commit_index(Adapter.key_metrics(server_id, 2_000))

      if is_integer(leader_index) and is_integer(member_index) and member_index < leader_index do
        Process.sleep(100)
        await_catch_up(leader, server_id, deadline)
      else
        :ok
      end
    end
  end

  defp commit_index(%{commit_index: index}) when is_integer(index), do: index
  defp commit_index(_), do: nil

  defp wait_for_server(target_node, name, timeout) do
    deadline = :erlang.monotonic_time(:millisecond) + timeout

    if target_node == node() do
      wait_for_local_server(name, deadline)
    else
      wait_for_remote_server(target_node, name, deadline)
    end
  end

  defp wait_for_local_server(name, deadline) do
    if Process.whereis(name) != nil do
      :ok
    else
      if :erlang.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        wait_for_local_server(name, deadline)
      else
        :ok
      end
    end
  end

  defp wait_for_remote_server(target_node, name, deadline) do
    registered? =
      try do
        :erpc.call(target_node, Process, :whereis, [name], 2_000) != nil
      catch
        _, _ -> false
      end

    if registered? do
      :ok
    else
      if :erlang.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        wait_for_remote_server(target_node, name, deadline)
      else
        :ok
      end
    end
  end

  # Start a member's server, replacing a stale/rival server with the same
  # registered name. A server that is not part of the leader's current member
  # set may be a leftover group from an earlier cluster view, so it cannot be
  # reused as-is.
  defp start_server(server_id, desired, cluster_name, partition_idx) do
    system = System.system_name()
    machine = Group.machine(partition_idx)

    case Adapter.start_server(system, cluster_name, server_id, machine, desired) do
      :ok ->
        :ok

      other ->
        Logger.info(
          "super_cache, raft.group, replacing stale server #{inspect(server_id)} " <>
            "after #{inspect(other)}"
        )

        _ = Adapter.stop_server(system, server_id)
        Process.sleep(50)

        case Adapter.start_server(system, cluster_name, server_id, machine, desired) do
          :ok ->
            :ok

          err ->
            Logger.warning(
              "super_cache, raft.group, start_server failed for #{inspect(server_id)}: " <>
                inspect(err)
            )

            {:error, err}
        end
    end
  end

  defp leave(server_id, leader) do
    if last_voter?(leader, server_id) do
      Logger.warning(
        "super_cache, raft.group, refusing to demote the last voter #{inspect(server_id)}"
      )

      {:error, :last_voter}
    else
      with {:ok, _, _} <- Adapter.demote_member(leader, server_id, @membership_timeout),
           {:ok, _, _} <- Adapter.remove_member(leader, server_id, @membership_timeout) do
        :ok
      else
        other ->
          Logger.warning("super_cache, raft.group, remove_member failed: #{inspect(other)}")
          {:error, other}
      end
    end
  end

  defp last_voter?(leader, server_id) do
    case Adapter.member_overview(leader, @membership_timeout) do
      %{cluster: cluster} when is_map(cluster) ->
        cluster
        |> Enum.reject(fn {id, _info} -> id == server_id end)
        |> Enum.all?(fn {_id, info} -> not voter?(info) end)

      _ ->
        false
    end
  end

  defp voter?(%{voter_status: %{membership: :voter}}), do: true
  defp voter?(%{membership: :voter}), do: true
  defp voter?(_), do: false

  defp stop_server(server_id) do
    _ = Adapter.stop_server(System.system_name(), server_id)
    :ok
  end

  # Catch-up repair guard: rebuild the cluster only when no member reports
  # committed log entries and the local partition holds no records (first
  # formation). Otherwise fail instead of discarding committed log with an
  # empty rebuild. Only the join-failure path uses this guard; departure and
  # crash paths reform directly so the group converges.
  defp safe_form_cluster(desired, cluster_name, partition_idx) do
    if committed_data?(desired, partition_idx) do
      Logger.warning(
        "super_cache, raft.group, refusing to reform #{inspect(cluster_name)} " <>
          "from scratch: committed data exists"
      )

      {:error, :committed_data_present}
    else
      form_cluster(desired, cluster_name, partition_idx)
    end
  end

  defp committed_data?(desired, partition_idx) do
    metrics_committed?(desired) or local_partition_has_records?(partition_idx)
  end

  defp metrics_committed?(desired) do
    Enum.any?(desired, &server_committed?/1)
  rescue
    _ -> false
  end

  defp server_committed?(server_id) do
    case Adapter.key_metrics(server_id, 2_000) do
      map when is_map(map) ->
        [:commit_index, :last_applied, :last_index, :last_written_index]
        |> Enum.any?(&committed_index?(Map.get(map, &1)))

      _ ->
        false
    end
  end

  defp committed_index?(idx) when is_integer(idx) and idx > 0, do: true
  defp committed_index?(_), do: false

  defp local_partition_has_records?(partition_idx) do
    case SuperCache.Partition.get_partition_by_idx(partition_idx) do
      nil -> false
      partition -> :ets.info(partition, :size) not in [nil, :undefined, 0]
    end
  rescue
    _ -> false
  end

  # No leader: stop every desired member's server (they may hold stale group
  # state) and bring the cluster back from the current partition map.
  # Each member recovers its persisted server when it has one and starts
  # fresh only when it never ran here, so surviving logs and terms are kept
  # instead of wiped. A member that genuinely never recovers still fails
  # loudly instead of hanging the group.
  defp form_cluster(desired, cluster_name, partition_idx) do
    Logger.info(
      "super_cache, raft.group, forming #{inspect(cluster_name)} with " <>
        "#{length(desired)} member(s)"
    )

    Enum.each(desired, &stop_server/1)
    Process.sleep(100)

    results =
      Enum.map(desired, &recover_or_start_server(&1, cluster_name, partition_idx, desired))

    case Enum.filter(results, &match?({:error, _}, &1)) do
      [] ->
        :ok

      failures ->
        Logger.warning("super_cache, raft.group, form cluster failed: #{inspect(failures)}")

        {:error, {:start_failed, failures}}
    end
  end

  defp recover_or_start_server(server_id, cluster_name, partition_idx, desired) do
    system = System.system_name()

    case Adapter.restart_server(system, server_id) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:ok, _, _} -> :ok
      {:error, _} -> start_fresh_server(server_id, cluster_name, partition_idx, desired)
    end
  end

  defp start_fresh_server(server_id, cluster_name, partition_idx, desired) do
    case Adapter.start_server(
           System.system_name(),
           cluster_name,
           server_id,
           Group.machine(partition_idx),
           desired
         ) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  defp await_members(_leader, _desired, remaining) when remaining <= 0 do
    Logger.warning("super_cache, raft.group, membership did not converge")
    {:error, :membership_not_converged}
  end

  defp await_members(leader, desired, remaining) do
    case Adapter.members(leader, 2_000) do
      {:ok, current, _leader} ->
        if Enum.sort(current) == Enum.sort(desired) do
          :ok
        else
          Process.sleep(100)
          await_members(leader, desired, remaining - 100)
        end

      _ ->
        Process.sleep(100)
        await_members(leader, desired, remaining - 100)
    end
  end
end
