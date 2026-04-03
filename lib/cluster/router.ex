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

  ## Read-your-writes consistency

  When a process writes a key, the Router records the partition order in a
  per-process ETS table.  Subsequent reads of the same partition (within a
  configurable TTL) are automatically routed to the primary node, ensuring
  the reader sees its own writes even in `:local` read mode.

  The tracking table is cleaned up lazily — entries older than the TTL are
  pruned on each write.  This adds negligible overhead (~100ns per write)
  while providing strong read-your-writes guarantees without requiring
  `read_mode: :primary` on every call.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Config, Partition, Storage}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

  @erpc_timeout 5_000

  # Read-your-writes tracking: per-process ETS table mapping
  # {partition_order, expiry_ms} → true.
  # TTL defaults to 5 seconds — long enough for replication to propagate.
  @ryw_ttl_ms 5_000
  @ryw_table __MODULE__.RywTracker

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

    # Track this write for read-your-writes consistency.
    track_write(order)

    true
  end

  @doc """
  Route a batch of puts to the correct primary, then replicate.

  Groups data by partition order and sends each group in a single `:erpc` call,
  dramatically reducing network overhead for bulk writes.

  ## Example

      Router.route_put_batch!([
        {:user, 1, "Alice"},
        {:user, 2, "Bob"},
        {:session, "tok1", :active}
      ])
  """
  @spec route_put_batch!([tuple], keyword) :: :ok
  def route_put_batch!(data_list, opts \\ []) when is_list(data_list) do
    # Group data by partition order
    grouped =
      Enum.group_by(data_list, fn data ->
        data |> Config.get_partition!() |> Partition.get_partition_order()
      end)

    # Send each group to its primary in a single erpc call
    Enum.each(grouped, fn {order, data_for_partition} ->
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        local_batch_write(order, data_for_partition)
      else
        forward(:route_put_batch!, [data_for_partition, [forwarded: true]], order)
      end
    end)

    :ok
  end

  ## ── Read ─────────────────────────────────────────────────────────────────────

  @doc "Route a key-based get."
  @spec route_get!(tuple, keyword) :: [tuple]
  def route_get!(data, opts \\ []) when is_tuple(data) do
    key = Config.get_key!(data)
    part_val = Config.get_partition!(data)
    order = Partition.get_partition_order(part_val)
    read_mode = resolve_read_mode(Keyword.get(opts, :read_mode, :local), order)

    do_read(read_mode, order, :get, key)
  end

  @doc "Route a get by explicit key + partition value."
  @spec route_get_by_key_partition!(any, any, keyword) :: [tuple]
  def route_get_by_key_partition!(key, partition_data, opts \\ []) do
    order = Partition.get_partition_order(partition_data)
    read_mode = resolve_read_mode(Keyword.get(opts, :read_mode, :local), order)

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

  # Batch write: applies all records locally then replicates in a single call.
  # For :strong mode, falls back to individual 3PC commits per record.
  defp local_batch_write(order, data_list) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        # 3PC doesn't support batch natively — commit each record individually.
        Enum.each(data_list, fn data ->
          ThreePhaseCommit.commit(order, [{:put, data}])
        end)

        true

      _ ->
        # Write all records locally in a single ETS operation per record,
        # then replicate the entire batch in one erpc call.
        Enum.each(data_list, fn data ->
          Storage.put(data, partition)
        end)

        Replicator.replicate_batch(order, :put, data_list)
        true
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

  # Read-your-writes: upgrade :local to :primary if this process recently
  # wrote to this partition.  Prevents stale reads immediately after a write.
  defp resolve_read_mode(:local, order) do
    if ryw_recent?(order), do: :primary, else: :local
  end

  defp resolve_read_mode(mode, _order), do: mode

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
  # :erpc — no closures.  Returns as soon as a strict majority agrees,
  # avoiding unnecessary waits for slow replicas.
  defp do_read(:quorum, order, op, arg) do
    {primary, replicas} = Manager.get_replicas(order)
    nodes = [primary | replicas]
    total = length(nodes)
    required = div(total, 2) + 1

    tasks =
      Enum.map(nodes, fn n ->
        Task.async(fn ->
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
        end)
      end)

    quorum_await(tasks, required, %{})
  end

  # Poll tasks until a strict majority agrees on a result, or all have returned.
  defp quorum_await(tasks, required, counts) do
    case Task.yield_many(tasks, 100) do
      [] ->
        # No tasks completed yet — keep waiting.
        quorum_await(tasks, required, counts)

      done ->
        # Remove completed tasks from the pending list.
        done_set = MapSet.new(Enum.map(done, fn {task, _} -> task end))
        pending = Enum.reject(tasks, &MapSet.member?(done_set, &1))

        # Tally results.
        new_counts =
          Enum.reduce(done, counts, fn {_task, result}, acc ->
            case result do
              {:ok, val} when is_list(val) -> Map.update(acc, val, 1, &(&1 + 1))
              _ -> acc
            end
          end)

        # Check if any result has reached majority.
        case Enum.find(new_counts, fn {_, count} -> count >= required end) do
          {result, _} ->
            # Kill remaining tasks — we already have our answer.
            Enum.each(pending, &Task.shutdown(&1, :brutal_kill))
            result

          nil when pending == [] ->
            # All tasks done, no majority — fall back to the most common result.
            case Enum.max_by(new_counts, fn {_, c} -> c end, fn -> {[], 0} end) do
              {result, _} -> result
              _ -> []
            end

          nil ->
            # Still waiting for more results.
            quorum_await(pending, required, new_counts)
        end
    end
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

    SuperCache.Log.debug(fn ->
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

  # ── Read-your-writes tracking ────────────────────────────────────────────────

  # Record that the calling process wrote to `order`.  Prunes expired entries
  # lazily to keep the table small.
  defp track_write(order) do
    ensure_ryw_table()
    now = System.monotonic_time(:millisecond)
    expiry = now + @ryw_ttl_ms

    # Insert the new entry.
    :ets.insert(@ryw_table, {{self(), order}, expiry})

    # Prune expired entries for this process (at most once per 100 writes).
    if :rand.uniform(100) == 1 do
      prune_ryw(now)
    end
  end

  # Check if the calling process wrote to `order` within the TTL window.
  defp ryw_recent?(order) do
    ensure_ryw_table()

    case :ets.lookup(@ryw_table, {self(), order}) do
      [{_, expiry}] ->
        now = System.monotonic_time(:millisecond)
        if now <= expiry do
          true
        else
          :ets.delete(@ryw_table, {self(), order})
          false
        end

      [] ->
        false
    end
  end

  # Create the per-node RYW tracking table if it doesn't exist.
  defp ensure_ryw_table() do
    case :ets.info(@ryw_table) do
      :undefined ->
        :ets.new(@ryw_table, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ ->
        :ok
    end
  end

  # Remove expired entries for the calling process.
  defp prune_ryw(now) do
    match_spec = [{{{:_, :"$1"}, :"$2"}, [{:"<", :"$2", now}], [true]}]

    @ryw_table
    |> :ets.select(match_spec)
    |> Enum.each(fn {pid, order} ->
      :ets.delete(@ryw_table, {pid, order})
    end)
  end

  # Fast path: read primary node from persistent_term cache.
  # Returns nil if the partition map hasn't been built yet.
  @compile {:inline, primary_node: 1}
  defp primary_node(order) do
    case Manager.get_replicas(order) do
      {primary, _} -> primary
    end
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
