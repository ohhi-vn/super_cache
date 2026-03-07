defmodule SuperCache.Distributed.KeyValue do
  @moduledoc """
  Cluster-aware key-value namespaces.

  Write replication mode is controlled by the cluster-wide `:replication_mode`
  setting (see `SuperCache.Cluster.Bootstrap`):

  | Mode      | Guarantee              | Extra latency    |
  |-----------|------------------------|------------------|
  | `:async`  | Eventual (default)     | None             |
  | `:sync`   | At-least-once delivery | +1 RTT per write |
  | `:strong` | Three-phase commit     | +3 RTTs per write|

  Reads default to the local node; pass `read_mode: :primary` or
  `read_mode: :quorum` when the local replica may be stale or absent.
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}
  require Logger

  ## Public API ─────────────────────────────────────────────────────────────────

  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, add key=#{inspect(key)}" end)
    route_write(kv_name, :local_put, [kv_name, key, value])
  end

  @spec get(any, any, any, keyword) :: any
  def get(kv_name, key, default \\ nil, opts \\ []) do
    route_read(kv_name, :local_get, [kv_name, key, default], opts)
  end

  @spec keys(any, keyword) :: [any]
  def keys(kv_name, opts \\ []) do
    route_read(kv_name, :local_keys, [kv_name], opts)
  end

  @spec values(any, keyword) :: [any]
  def values(kv_name, opts \\ []) do
    route_read(kv_name, :local_values, [kv_name], opts)
  end

  @spec count(any, keyword) :: non_neg_integer
  def count(kv_name, opts \\ []) do
    route_read(kv_name, :local_count, [kv_name], opts)
  end

  @spec to_list(any, keyword) :: [{any, any}]
  def to_list(kv_name, opts \\ []) do
    route_read(kv_name, :local_to_list, [kv_name], opts)
  end

  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    Logger.debug(fn -> "super_cache, dist.kv #{inspect(kv_name)}, remove key=#{inspect(key)}" end)
    route_write(kv_name, :local_delete, [kv_name, key])
  end

  @spec remove_all(any) :: :ok
  def remove_all(kv_name) do
    route_write(kv_name, :local_delete_all, [kv_name])
  end

  ## Remote entry points — writes ───────────────────────────────────────────────

  @doc false
  def local_put(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    apply_write(idx, partition, [{:put, {{:kv, kv_name, key}, value}}])
    true
  end

  @doc false
  def local_delete(kv_name, key) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    apply_write(idx, partition, [{:delete, {:kv, kv_name, key}}])
    :ok
  end

  @doc false
  def local_delete_all(kv_name) do
    partition = Partition.get_partition(kv_name)
    idx = Partition.get_partition_order(kv_name)
    apply_write(idx, partition, [{:delete_match, {{:kv, kv_name, :_}, :_}}])
    :ok
  end

  ## Remote entry points — reads ────────────────────────────────────────────────

  @doc false
  def local_get(kv_name, key, default) do
    partition = Partition.get_partition(kv_name)

    case Storage.get({:kv, kv_name, key}, partition) do
      [] -> default
      [{_, value}] -> value
    end
  end

  @doc false
  def local_keys(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, _} -> key end)
  end

  @doc false
  def local_values(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, _}, value} -> value end)
  end

  @doc false
  def local_count(kv_name), do: do_match(kv_name) |> length()

  @doc false
  def local_to_list(kv_name) do
    do_match(kv_name)
    |> Enum.map(fn {{:kv, ^kv_name, key}, value} -> {key, value} end)
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp do_match(kv_name) do
    partition = Partition.get_partition(kv_name)
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, partition)
  end

  # ── Write routing ──────────────────────────────────────────────────────────

  defp route_write(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.kv #{inspect(kv_name)}, fwd #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # Dispatch to 3PC or async replication depending on cluster replication_mode.
  # In :strong mode, ThreePhaseCommit.commit/2 handles both local Storage apply
  # (via apply_local) and replica propagation atomically.
  # In all other modes, Storage is written locally first, then replicated async.
  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("super_cache, dist.kv, 3pc failed: #{inspect(reason)}")
            {:error, reason}
        end

      _ ->
        Enum.each(ops, fn
          {:put, record} ->
            Storage.put(record, partition)
            Replicator.replicate(idx, :put, record)

          {:delete, key} ->
            Storage.delete(key, partition)
            Replicator.replicate(idx, :delete, key)

          {:delete_match, pattern} ->
            Storage.delete_match(pattern, partition)
            Replicator.replicate(idx, :delete_match, pattern)

          {:delete_all, _} ->
            Storage.delete_all(partition)
            Replicator.replicate(idx, :delete_all, nil)
        end)

        :ok
    end
  end

  # ── Read routing ───────────────────────────────────────────────────────────

  defp route_read(kv_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)

    effective_mode =
      if mode == :local and not has_partition?(kv_name), do: :primary, else: mode

    case effective_mode do
      :local -> apply(__MODULE__, fun, args)
      :primary -> route_read_primary(kv_name, fun, args)
      :quorum -> route_read_quorum(kv_name, fun, args)
    end
  end

  defp has_partition?(kv_name) do
    idx = Partition.get_partition_order(kv_name)
    {primary, replicas} = Manager.get_replicas(idx)
    node() in [primary | replicas]
  end

  defp route_read_primary(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, _} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.kv #{inspect(kv_name)}, read_primary #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read_quorum(kv_name, fun, args) do
    idx = Partition.get_partition_order(kv_name)
    {primary, replicas} = Manager.get_replicas(idx)

    results =
      [primary | replicas]
      |> Task.async_stream(
        fn
          n when n == node() -> apply(__MODULE__, fun, args)
          n -> :erpc.call(n, __MODULE__, fun, args, 5_000)
        end,
        timeout: 5_000,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, result} -> [result]
        _ -> []
      end)

    quorum_result(results, fn ->
      if primary == node(),
        do: apply(__MODULE__, fun, args),
        else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end)
  end

  defp quorum_result(results, tiebreak_fn) do
    majority = div(length(results), 2) + 1

    case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
      {result, _} -> result
      nil -> tiebreak_fn.()
    end
  end
end
