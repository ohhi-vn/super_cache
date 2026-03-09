defmodule SuperCache.Cluster.Router do
  @moduledoc """
  Routes SuperCache operations to the correct primary node and applies
  replication after each write.

  ## Routing contract

  1. Determine the **partition order** (integer index) for the operation from
     the data tuple or explicit partition argument via
     `Partition.get_partition_order/1`.
  2. Look up `{primary, replicas}` from `Manager.get_replicas/1`
     (zero-cost `:persistent_term` read).
  3. If `node() == primary` → apply locally, then call `Replicator.replicate/3`.
  4. Otherwise → forward the entire operation to the primary via `:erpc`,
     which applies and replicates it.  Forwarded calls never forward again
     (detected via a `:forwarded` flag in opts) to prevent cycles.

  ## Anti-cycle guard

  Every outbound `:erpc` call appends `forwarded: true` to its opts list.
  A function that receives `forwarded: true` always executes locally and
  skips the primary check, preventing infinite forwarding chains when the
  partition map is momentarily inconsistent.

  ## No anonymous functions across node boundaries

  All `:erpc` calls pass only plain, serializable Erlang terms — integers,
  atoms, and tuples.  Anonymous functions (closures) are never passed via
  `:erpc` because Erlang fun serialization is fragile: the remote node must
  have the identical module version, otherwise the call raises `badfun`.
  Instead, every remote read goes through the explicit public dispatcher
  `local_read/3`, which takes an operation atom (`:get | :match | :match_object`)
  and a plain argument.

  ## 3PC writes

  When `Manager.replication_mode/0` returns `:strong`, writes are handed
  to `ThreePhaseCommit.commit/2` on the primary instead of the normal
  local-write + async/sync replicate path.
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

  @erpc_timeout 5_000

  ## ── Write ────────────────────────────────────────────────────────────────────

  @doc "Route a put to the correct primary, then replicate."
  @spec route_put!(tuple, keyword) :: true
  def route_put!(data, opts \\ []) when is_tuple(data) do
    order = get_partition_order(data)

    if primary?(order) or Keyword.get(opts, :forwarded, false) do
      local_write(order, :put, data)
    else
      forward(:route_put!, [data, [forwarded: true]], order)
    end
  end

  ## ── Read ─────────────────────────────────────────────────────────────────────

  @doc "Route a key-based get."
  @spec route_get!(tuple, keyword) :: [tuple]
  def route_get!(data, opts \\ []) when is_tuple(data) do
    key = Config.get_key!(data)
    part_val = Config.get_partition!(data)
    order = Partition.get_partition_order(part_val)
    read_mode = Keyword.get(opts, :read_mode, :local)

    do_read(read_mode, order, :get, key)
  end

  @doc "Route a get by explicit key + partition value."
  @spec route_get_by_key_partition!(any, any, keyword) :: [tuple]
  def route_get_by_key_partition!(key, partition_data, opts \\ []) do
    order = Partition.get_partition_order(partition_data)
    read_mode = Keyword.get(opts, :read_mode, :local)

    do_read(read_mode, order, :get, key)
  end

  @doc "Route a match-pattern scan across one or all partitions."
  @spec route_get_by_match!(any, tuple, keyword) :: [[any]]
  def route_get_by_match!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    read_mode = Keyword.get(opts, :read_mode, :local)
    fan_read(partition_data, read_mode, :match, pattern)
  end

  @doc "Route a match-object scan across one or all partitions."
  @spec route_get_by_match_object!(any, tuple, keyword) :: [tuple]
  def route_get_by_match_object!(partition_data, pattern, opts \\ []) when is_tuple(pattern) do
    read_mode = Keyword.get(opts, :read_mode, :local)
    fan_read(partition_data, read_mode, :match_object, pattern)
  end

  @doc "Fold over local ETS — always local, never forwarded."
  @spec route_scan!(any, (any, any -> any), any) :: any
  def route_scan!(partition_data, fun, acc) when is_function(fun, 2) do
    resolve_partitions(partition_data)
    |> Enum.reduce(acc, fn p, result -> Storage.scan(fun, result, p) end)
  end

  ## ── Remote read entry-point (called via :erpc — NO closures) ─────────────────

  @doc false
  # Public so it can be invoked via :erpc from do_read/4 and quorum_read/3.
  # Accepts only plain serializable terms — no anonymous functions.
  #
  # op:
  #   :get          → Storage.get(arg, partition)
  #   :match        → Storage.get_by_match(arg, partition)
  #   :match_object → Storage.get_by_match_object(arg, partition)
  @spec local_read(non_neg_integer, :get | :match | :match_object, any) :: list
  def local_read(order, op, arg) do
    partition = Partition.get_partition_by_idx(order)

    case op do
      :get -> Storage.get(arg, partition)
      :match -> Storage.get_by_match(arg, partition)
      :match_object -> Storage.get_by_match_object(arg, partition)
    end
  end

  ## ── Delete ───────────────────────────────────────────────────────────────────

  @doc "Route a key-based delete to the correct primary."
  @spec route_delete!(tuple, keyword) :: :ok
  def route_delete!(data, opts \\ []) when is_tuple(data) do
    order = get_partition_order(data)

    if primary?(order) or Keyword.get(opts, :forwarded, false) do
      key = Config.get_key!(data)
      local_delete(order, key)
    else
      forward(:route_delete!, [data, [forwarded: true]], order)
    end

    :ok
  end

  @doc "Delete all records — one routed call per partition."
  @spec route_delete_all() :: :ok
  def route_delete_all() do
    num = Config.get_config(:num_partition, Partition.get_schedulers())

    0..(num - 1)
    |> Enum.each(fn order ->
      if primary?(order) do
        local_delete_all(order)
      else
        {primary, _} = Manager.get_replicas(order)
        safe_erpc(primary, __MODULE__, :route_delete_all_partition, [order, [forwarded: true]])
      end
    end)

    :ok
  end

  @doc false
  # Single-partition delete_all; called via :erpc from route_delete_all/0.
  @spec route_delete_all_partition(non_neg_integer, keyword) :: :ok
  def route_delete_all_partition(order, opts \\ []) do
    if primary?(order) or Keyword.get(opts, :forwarded, false) do
      local_delete_all(order)
    else
      forward(:route_delete_all_partition, [order, [forwarded: true]], order)
    end

    :ok
  end

  @doc "Route a match-based delete, one partition order at a time."
  @spec route_delete_match!(any, tuple) :: :ok
  def route_delete_match!(partition_data, pattern) when is_tuple(pattern) do
    resolve_partition_orders(partition_data)
    |> Enum.each(fn order ->
      if primary?(order) do
        local_delete_match(order, pattern)
      else
        {primary, _} = Manager.get_replicas(order)

        safe_erpc(primary, __MODULE__, :route_delete_match_partition!, [
          order,
          pattern,
          [forwarded: true]
        ])
      end
    end)

    :ok
  end

  @doc false
  # Single-partition delete_match; called via :erpc from route_delete_match!/2.
  @spec route_delete_match_partition!(non_neg_integer, tuple, keyword) :: :ok
  def route_delete_match_partition!(order, pattern, opts \\ []) do
    if primary?(order) or Keyword.get(opts, :forwarded, false) do
      local_delete_match(order, pattern)
    else
      forward(:route_delete_match_partition!, [order, pattern, [forwarded: true]], order)
    end

    :ok
  end

  @doc "Route a delete by explicit key + partition value to the correct primary."
  @spec route_delete_by_key_partition!(any, any, keyword) :: :ok
  def route_delete_by_key_partition!(key, partition_data, opts \\ []) do
    order = Partition.get_partition_order(partition_data)

    if primary?(order) or Keyword.get(opts, :forwarded, false) do
      local_delete(order, key)
    else
      forward(:route_delete_by_key_partition!, [key, partition_data, [forwarded: true]], order)
    end

    :ok
  end

  ## ── Private — local write helpers ────────────────────────────────────────────

  defp local_write(order, op, data) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        # 3PC applies locally inside ThreePhaseCommit.apply_local/2
        ThreePhaseCommit.commit(order, [{op, data}])
        true

      _ ->
        result = Storage.put(data, partition)
        Replicator.replicate(order, op, data)
        result
    end
  end

  defp local_delete(order, key) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        ThreePhaseCommit.commit(order, [{:delete, key}])

      _ ->
        Storage.delete(key, partition)
        Replicator.replicate(order, :delete, key)
    end

    :ok
  end

  defp local_delete_all(order) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        ThreePhaseCommit.commit(order, [{:delete_all, nil}])

      _ ->
        Storage.delete_all(partition)
        Replicator.replicate(order, :delete_all, nil)
    end

    :ok
  end

  defp local_delete_match(order, pattern) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        ThreePhaseCommit.commit(order, [{:delete_match, pattern}])

      _ ->
        Storage.delete_match(pattern, partition)
        Replicator.replicate(order, :delete_match, pattern)
    end

    :ok
  end

  ## ── Private — single-partition read dispatcher ───────────────────────────────

  # Local: read directly from the local ETS table — no network hop.
  defp do_read(:local, order, op, arg) do
    local_read(order, op, arg)
  end

  # Primary: if this node IS the primary, read locally; otherwise forward to
  # the primary via :erpc passing only plain terms (op atom + arg), never
  # a closure.
  defp do_read(:primary, order, op, arg) do
    if primary?(order) do
      local_read(order, op, arg)
    else
      {primary, _} = Manager.get_replicas(order)
      result = safe_erpc(primary, __MODULE__, :local_read, [order, op, arg])

      case result do
        list when is_list(list) ->
          list

        {:error, reason} ->
          Logger.warning(
            "super_cache, router, primary read failed (order=#{order}): #{inspect(reason)}"
          )

          []
      end
    end
  end

  # Quorum: ask primary + all replicas in parallel; only plain terms sent via
  # :erpc — no closures.  Majority vote decides the result.
  defp do_read(:quorum, order, op, arg) do
    {primary, replicas} = Manager.get_replicas(order)
    nodes = [primary | replicas]

    results =
      nodes
      |> Task.async_stream(
        fn n ->
          try do
            if n == node() do
              local_read(order, op, arg)
            else
              :erpc.call(n, __MODULE__, :local_read, [order, op, arg], @erpc_timeout)
            end
          catch
            kind, reason ->
              Logger.warning(
                "super_cache, router, quorum read failed on #{inspect(n)}: " <>
                  inspect({kind, reason})
              )

              :error
          end
        end,
        timeout: @erpc_timeout + 500,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, list} when is_list(list) -> [list]
        _ -> []
      end)

    quorum_merge(results)
  end

  ## ── Private — multi-partition fan-out ────────────────────────────────────────

  # Local fan: all partitions resolved to ETS table atoms, no :erpc involved.
  defp fan_read(partition_data, :local, op, arg) do
    resolve_partitions(partition_data)
    |> Enum.flat_map(fn p ->
      case op do
        :get -> Storage.get(arg, p)
        :match -> Storage.get_by_match(arg, p)
        :match_object -> Storage.get_by_match_object(arg, p)
      end
    end)
  end

  # Primary / quorum fan: resolve partition orders (integers), then for each
  # order delegate to do_read/4 which handles routing with plain-term :erpc.
  defp fan_read(partition_data, mode, op, arg) do
    resolve_partition_orders(partition_data)
    |> Task.async_stream(
      fn order -> do_read(mode, order, op, arg) end,
      timeout: @erpc_timeout + 500
    )
    |> Enum.flat_map(fn
      {:ok, list} when is_list(list) -> list
      _ -> []
    end)
  end

  ## ── Private — quorum resolution ─────────────────────────────────────────────

  # Return the result agreed on by ≥ ⌈n/2⌉ nodes; fall back to the first
  # result when no strict majority exists.
  defp quorum_merge([]), do: []

  defp quorum_merge(results) do
    total = length(results)
    required = div(total, 2) + 1

    case Enum.group_by(results, & &1) |> Enum.find(fn {_, g} -> length(g) >= required end) do
      {result, _} -> result
      nil -> hd(results)
    end
  end

  ## ── Private — forwarding helpers ─────────────────────────────────────────────

  defp forward(fun_name, args, order) do
    {primary, _} = Manager.get_replicas(order)

    Logger.debug(fn ->
      "super_cache, router, forwarding #{fun_name} → primary #{inspect(primary)}"
    end)

    safe_erpc(primary, __MODULE__, fun_name, args)
  end

  defp safe_erpc(target, mod, fun, args) do
    try do
      :erpc.call(target, mod, fun, args, @erpc_timeout)
    catch
      kind, reason ->
        Logger.warning(
          "super_cache, router, erpc #{inspect(fun)} failed → #{inspect(target)}: " <>
            inspect({kind, reason})
        )

        {:error, {kind, reason}}
    end
  end

  ## ── Private — partition resolution ──────────────────────────────────────────

  defp primary?(order) do
    {primary, _} = Manager.get_replicas(order)
    primary == node()
  end

  defp get_partition_order(data) do
    data |> Config.get_partition!() |> Partition.get_partition_order()
  end

  # Returns ETS table atoms — used by local reads and scans.
  defp resolve_partitions(:_), do: Partition.get_all_partition() |> List.flatten()
  defp resolve_partitions(data), do: [Partition.get_partition(data)]

  # Returns integer partition orders — used by routed / fan-out operations.
  defp resolve_partition_orders(:_) do
    num = Config.get_config(:num_partition, Partition.get_schedulers())
    Enum.to_list(0..(num - 1))
  end

  defp resolve_partition_orders(data) do
    [Partition.get_partition_order(data)]
  end
end
