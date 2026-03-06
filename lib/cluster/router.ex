defmodule SuperCache.Cluster.Router do
  @moduledoc """
  Routes every cache operation to the correct primary node.

  ## Critical design rule
  Every `route_*/1` function is the **public entry point** — it decides
  whether to execute locally or forward to a remote primary.

  Every `local_*/1` function is the **execution kernel** — it writes to
  the local ETS table and fans out replication.  It never inspects the
  partition map and never forwards anywhere.

  Remote entry points (`remote_*`) called via `:erpc` delegate ONLY to
  `local_*` functions, never back to `route_*`.  This breaks the forwarding
  cycle: node1 → node2 → node1 → ... that caused the timeout loop.
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage}
  alias SuperCache.Cluster.{Manager, Replicator}

  @forward_timeout 5_000
  @bulk_timeout    10_000

  ## Public routing API ────────────────────────────────────────────────────────

  @spec route_put!(tuple) :: true
  def route_put!(data) when is_tuple(data) do
    {idx, partition} = resolve(data)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      local_put(data, idx, partition)
    else
      Logger.debug(fn -> "super_cache, router, put → #{inspect(primary)}" end)
      forward_sync!(primary, :remote_put, [data], @forward_timeout)
    end
  end

  @spec route_delete!(tuple) :: :ok
  def route_delete!(data) when is_tuple(data) do
    {idx, partition} = resolve(data)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      local_delete(Config.get_key!(data), idx, partition)
    else
      Logger.debug(fn -> "super_cache, router, delete → #{inspect(primary)}" end)
      forward_sync!(primary, :remote_delete, [data], @forward_timeout)
    end
  end

  @spec route_delete_by_key_partition!(any, any) :: :ok
  def route_delete_by_key_partition!(key, partition_data) do
    idx = Partition.get_partition_order(partition_data)
    partition = Partition.get_partition_by_idx(idx)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      local_delete(key, idx, partition)
    else
      forward_sync!(primary, :remote_delete_by_kp, [key, partition_data], @forward_timeout)
    end
  end

  @spec route_delete_match!(any, tuple) :: :ok
  def route_delete_match!(partition_data, pattern) when is_tuple(pattern) do
    {local_pairs, remote_groups} =
      partition_data
      |> partitions_with_idx()
      |> split_by_primary()

    Enum.each(local_pairs, fn {idx, partition} ->
      local_delete_match(pattern, idx, partition)
    end)

    forward_concurrent(remote_groups, :remote_delete_match, [partition_data, pattern], @bulk_timeout)
  end

  @spec route_delete_all() :: :ok
  def route_delete_all() do
    {local_pairs, remote_groups} =
      0..(Partition.get_num_partition() - 1)
      |> Enum.map(fn idx -> {idx, Partition.get_partition_by_idx(idx)} end)
      |> split_by_primary()

    Enum.each(local_pairs, fn {idx, partition} ->
      local_delete_all(idx, partition)
    end)

    forward_concurrent(remote_groups, :remote_delete_all, [], @bulk_timeout)
  end

  @spec route_get!(tuple, keyword) :: [tuple]
  def route_get!(data, opts \\ []) when is_tuple(data) do
    {idx, partition} = resolve(data)
    key = Config.get_key!(data)
    {primary, replicas} = Manager.get_replicas(idx)

    case Keyword.get(opts, :read_mode, :local) do
      :local ->
        Storage.get(key, partition)

      :primary ->
        if primary == node() do
          Storage.get(key, partition)
        else
          Logger.debug(fn -> "super_cache, router, get → #{inspect(primary)}" end)
          forward_sync!(primary, :remote_get, [data, opts], @forward_timeout)
        end

      :quorum ->
        quorum_read(key, partition, primary, replicas)
    end
  end

  ## Remote entry points ───────────────────────────────────────────────────────
  # Called via :erpc from another node.
  # MUST delegate to local_* only — never to route_* — to prevent
  # forwarding loops where two nodes keep bouncing calls at each other.

  @doc false
  def remote_put(data) do
    {idx, partition} = resolve(data)
    local_put(data, idx, partition)
  end

  def remote_delete(data) do
    {idx, partition} = resolve(data)
    local_delete(Config.get_key!(data), idx, partition)
  end

  def remote_get(data, opts) do
    {_idx, partition} = resolve(data)
    Storage.get(Config.get_key!(data), partition)
    |> maybe_apply_read_opts(data, opts)
  end

  def remote_delete_by_kp(key, partition_data) do
    idx = Partition.get_partition_order(partition_data)
    partition = Partition.get_partition_by_idx(idx)
    local_delete(key, idx, partition)
  end

  def remote_delete_match(partition_data, pattern) do
    # Execute only on local partitions — no routing back out.
    partition_data
    |> partitions_with_idx()
    |> Enum.each(fn {_idx, partition} ->
      Storage.delete_match(pattern, partition)
    end)
    :ok
  end

  def remote_delete_all() do
    # Wipe every local partition unconditionally — no routing, no primary
    # check, no erpc.  The caller (safe_delete_all in tests, or
    # route_delete_all on a non-primary) has already decided this node
    # should clear its data.
    0..(Partition.get_num_partition() - 1)
    |> Enum.each(fn idx ->
      partition = Partition.get_partition_by_idx(idx)
      Storage.delete_all(partition)
    end)
    :ok
  end

  ## Local execution kernels ───────────────────────────────────────────────────
  # Write to local ETS and fan out to replicas.
  # Never forward. Never inspect primary/replica map for routing.

  defp local_put(data, idx, partition) do
    Storage.put(data, partition)
    Replicator.replicate(idx, :put, data)
    true
  end

  defp local_delete(key, idx, partition) do
    Storage.delete(key, partition)
    Replicator.replicate(idx, :delete, key)
    :ok
  end

  defp local_delete_match(pattern, idx, partition) do
    Storage.delete_match(pattern, partition)
    Replicator.replicate(idx, :delete_match, pattern)
    :ok
  end

  defp local_delete_all(idx, partition) do
    Storage.delete_all(partition)
    Replicator.replicate(idx, :delete_all, nil)
    :ok
  end

  ## Private — forwarding ──────────────────────────────────────────────────────

  defp forward_sync!(target, fun, args, timeout) do
    try do
      :erpc.call(target, __MODULE__, fun, args, timeout)
    catch
      :error, {:erpc, :timeout} ->
        raise RuntimeError,
          "super_cache, router, timeout forwarding #{fun} to #{inspect(target)}"
      :error, {:erpc, reason} ->
        raise RuntimeError,
          "super_cache, router, erpc #{fun} to #{inspect(target)} failed: #{inspect(reason)}"
    end
  end

  defp forward_concurrent(remote_groups, fun, args, timeout) do
    remote_groups
    |> Enum.map(fn {target, _pairs} ->
      Task.async(fn ->
        try do
          {:ok, :erpc.call(target, __MODULE__, fun, args, timeout)}
        catch
          :error, {:erpc, :timeout} ->
            Logger.warning("super_cache, router, timeout on #{fun} to #{inspect(target)}")
            {:error, target, :timeout}
          :error, {:erpc, reason} ->
            Logger.warning("super_cache, router, #{fun} failed on #{inspect(target)}: #{inspect(reason)}")
            {:error, target, reason}
        end
      end)
    end)
    |> Task.await_many(timeout + 1_000)
    :ok
  end

  ## Private — helpers ─────────────────────────────────────────────────────────

  defp resolve(data) do
    partition_data = Config.get_partition!(data)
    idx            = Partition.get_partition_order(partition_data)
    partition      = Partition.get_partition_by_idx(idx)
    {idx, partition}
  end

  defp partitions_with_idx(:_) do
    Enum.map(0..(Partition.get_num_partition() - 1), fn idx ->
      {idx, Partition.get_partition_by_idx(idx)}
    end)
  end

  defp partitions_with_idx(partition_data) do
    idx = Partition.get_partition_order(partition_data)
    [{idx, Partition.get_partition_by_idx(idx)}]
  end

  defp split_by_primary(pairs) do
    me = node()
    {local, remote} =
      Enum.split_with(pairs, fn {idx, _} ->
        {primary, _} = Manager.get_replicas(idx)
        primary == me
      end)

    remote_groups =
      remote
      |> Enum.group_by(fn {idx, _} ->
        {primary, _} = Manager.get_replicas(idx)
        primary
      end)
      |> Enum.to_list()

    {local, remote_groups}
  end

  defp maybe_apply_read_opts(result, _data, _opts), do: result

  defp quorum_read(key, local_partition, primary, replicas) do
    all_nodes = Enum.uniq([primary | replicas])
    me        = node()

    all_nodes
    |> Enum.map(fn n ->
      Task.async(fn ->
        if n == me do
          {n, Storage.get(key, local_partition)}
        else
          try do
            {n, :erpc.call(n, Storage, :get, [key, local_partition], 3_000)}
          catch
            _, _ -> {n, :error}
          end
        end
      end)
    end)
    |> Task.await_many(4_000)
    |> Enum.reject(fn {_, r} -> r == :error end)
    |> Enum.frequencies_by(fn {_, v} -> v end)
    |> Enum.max_by(fn {_, count} -> count end, fn -> {[], 0} end)
    |> elem(0)
  end
end
