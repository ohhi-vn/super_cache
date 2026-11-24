defmodule SuperCache.Cluster.Raft.Group do
  @moduledoc """
  Maps SuperCache partitions to Raft server groups.

  Each partition gets one Raft cluster named after the partition. The voting
  members are the partition's primary and replicas from
  `SuperCache.Cluster.Manager`, so all nodes sharing a partition map derive
  the same membership. The primary node creates the group; replica nodes host
  their server and wait for it to become available.
  """

  require Logger

  alias SuperCache.Cluster.{Manager, Raft}
  alias SuperCache.Cluster.Raft.Adapter
  alias SuperCache.Config

  @groups_key {__MODULE__, :groups}
  @server_prefix :super_cache_raft_partition

  @doc "Return the local Raft server name for a partition."
  @spec name(non_neg_integer) :: atom
  def name(partition_idx), do: :"#{@server_prefix}_#{partition_idx}"

  @doc "Return the cluster name for a partition."
  @spec cluster_name(non_neg_integer) :: atom
  def cluster_name(partition_idx), do: name(partition_idx)

  @doc "Return this node's server id for a partition."
  @spec server_id(non_neg_integer) :: {atom, node}
  def server_id(partition_idx), do: {name(partition_idx), node()}

  @doc "Return the state machine tuple for a partition."
  @spec machine(non_neg_integer) :: tuple
  def machine(partition_idx), do: {:machine, Raft.Machine, %{partition_idx: partition_idx}}

  @doc """
  Return all server ids for a partition, in deterministic
  `{name, node}` order derived from the partition map.
  """
  @spec member_ids(non_neg_integer) :: [{atom, node}]
  def member_ids(partition_idx),
    do: members_from(Manager.get_replicas(partition_idx), partition_idx)

  @doc "Derive deterministic member ids from a `{primary, replicas}` tuple."
  @spec members_from({node, [node]}, non_neg_integer) :: [{atom, node}]
  def members_from({primary, replicas}, partition_idx) do
    [primary | replicas]
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(&{name(partition_idx), &1})
  end

  @doc "Return `{to_add, to_remove}` between current and desired member ids."
  @spec diff_members([term], [term]) :: {[term], [term]}
  def diff_members(current, desired), do: {desired -- current, current -- desired}

  @doc "Return the partition indexes this node currently owns."
  @spec local_partitions() :: [non_neg_integer]
  def local_partitions do
    num = Config.get_config(:num_partition, 0) || 0

    if num <= 0 do
      []
    else
      0..(num - 1)
      |> Enum.filter(fn idx ->
        {primary, replicas} = Manager.get_replicas(idx)
        node() in [primary | replicas]
      end)
    end
  end

  @doc "Start the Raft group for a partition."
  @spec start(non_neg_integer) :: :ok | {:error, term}
  def start(partition_idx) do
    {primary, _replicas} = Manager.get_replicas(partition_idx)

    result =
      if primary == node() do
        Adapter.start_or_restart_cluster(
          Raft.System.system_name(),
          cluster_name(partition_idx),
          machine(partition_idx),
          member_ids(partition_idx),
          Raft.Config.get(:command_timeout_ms)
        )
      else
        await_local_server(partition_idx)
      end

    # `start_or_restart_cluster` can report :cluster_not_formed when a restart
    # already brought the local server up. Registration is the source of
    # truth for whether this node can serve the group.
    case await_local_server(partition_idx, 2_000) do
      :ok ->
        case result do
          {:error, _} ->
            Logger.warning(
              "super_cache, raft.group, start reported #{inspect(result)} but server is running"
            )

          _ ->
            :ok
        end

        track(partition_idx)

      {:error, _} = err ->
        err
    end
  end

  @doc "Ensure this node has a started group for a partition."
  @spec ensure_started(non_neg_integer) :: :ok | {:error, term}
  def ensure_started(partition_idx) do
    if partition_idx in tracked() do
      :ok
    else
      start(partition_idx)
    end
  end

  @doc "Start groups for every partition owned by this node."
  @spec start_all() :: :ok
  def start_all do
    Enum.each(local_partitions(), &start/1)
    :ok
  end

  @doc "Stop the local Raft server for a partition."
  @spec stop(non_neg_integer) :: :ok | {:error, term}
  def stop(partition_idx) do
    result = Adapter.stop_server(Raft.System.system_name(), server_id(partition_idx))

    if Process.whereis(name(partition_idx)) == nil do
      untrack(partition_idx)
      :ok
    else
      Logger.warning(
        "super_cache, raft.group, failed to stop partition #{partition_idx}: #{inspect(result)}"
      )

      {:error, :server_still_running}
    end
  end

  @doc "Stop every local group started by this node."
  @spec stop_all() :: :ok
  def stop_all do
    Enum.each(tracked(), &stop/1)
    :ok
  end

  @doc "Return the partition indexes with a local group started."
  @spec tracked() :: [non_neg_integer]
  def tracked, do: :persistent_term.get(@groups_key, [])

  @doc "Forget all tracked groups without stopping their servers."
  @spec untrack_all() :: :ok
  def untrack_all do
    :persistent_term.put(@groups_key, [])
    :ok
  end

  defp track(idx) do
    :persistent_term.put(@groups_key, tracked() |> List.delete(idx) |> List.insert_at(-1, idx))
    :ok
  end

  defp untrack(idx) do
    :persistent_term.put(@groups_key, List.delete(tracked(), idx))
    :ok
  end

  defp await_local_server(partition_idx, timeout \\ 5_000) do
    await_local_server_loop(partition_idx, System.monotonic_time(:millisecond) + timeout)
  end

  defp await_local_server_loop(partition_idx, deadline) do
    if Process.whereis(name(partition_idx)) != nil do
      :ok
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining > 0 do
        Process.sleep(10)
        await_local_server_loop(partition_idx, deadline)
      else
        {:error, :server_not_started}
      end
    end
  end
end
