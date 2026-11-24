defmodule SuperCache.Cluster.Raft.Group.Reconciler do
  @moduledoc """
  Serializes Raft membership changes and limits them to the partition's
  primary node, so concurrent cluster events cannot issue overlapping
  join/leave operations for the same Raft group.

  Membership is reconciled in two ways:

    * when the group has a leader, missing members are started and joined and
      stale members are removed;
    * when the group has no leader (first formation or a lost quorum), the
      primary stops the desired members' servers and forms the cluster again
      from the current partition map.

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
        case reconcile_with_leader(leader, desired, cluster_name, partition_idx) do
          {:error, :noproc} -> form_cluster(desired, cluster_name, partition_idx)
          other -> other
        end

      {:error, _} ->
        form_cluster(desired, cluster_name, partition_idx)
    end
  end

  defp reconcile_with_leader(leader, desired, cluster_name, partition_idx) do
    case Adapter.members(leader, @membership_timeout) do
      {:ok, current, _leader} ->
        {to_add, to_remove} = Group.diff_members(current, desired)

        Enum.each(to_remove, &leave(&1, leader))
        Enum.each(to_add, &join(&1, leader, desired, cluster_name, partition_idx))

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
  end

  defp join(server_id, leader, desired, cluster_name, partition_idx) do
    case start_server(server_id, desired, cluster_name, partition_idx) do
      :ok ->
        case Adapter.add_member(leader, server_id, @membership_timeout) do
          {:ok, _, _} ->
            :ok

          other ->
            Logger.warning("super_cache, raft.group, add_member failed: #{inspect(other)}")
            {:error, other}
        end

      {:error, _} = err ->
        err
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
    case Adapter.remove_member(leader, server_id, @membership_timeout) do
      {:ok, _, _} ->
        :ok

      other ->
        Logger.warning("super_cache, raft.group, remove_member failed: #{inspect(other)}")
        {:error, other}
    end
  end

  defp stop_server(server_id) do
    _ = Adapter.stop_server(System.system_name(), server_id)
    :ok
  end

  # No leader: stop every desired member's server (they may hold stale group
  # state) and form the cluster again from the current partition map.
  defp form_cluster(desired, cluster_name, partition_idx) do
    Logger.info(
      "super_cache, raft.group, forming #{inspect(cluster_name)} with " <>
        "#{length(desired)} member(s)"
    )

    Enum.each(desired, &stop_server/1)
    Process.sleep(100)

    case Adapter.start_cluster(
           System.system_name(),
           cluster_name,
           Group.machine(partition_idx),
           desired,
           @membership_timeout
         ) do
      {:ok, _started, _not_started} ->
        :ok

      other ->
        Logger.warning("super_cache, raft.group, form cluster failed: #{inspect(other)}")
        {:error, other}
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
