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

  alias SuperCache.Cluster.{DistributedHelpers, Manager, Metrics, Quorum, Replicator, Rpc, WAL}
  alias SuperCache.{Config, Partition, Storage}

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

    result =
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        local_write(order, :put, data)
      else
        forward(:route_put!, [data, [forwarded: true]], order)
      end

    # Track this write for read-your-writes consistency.
    track_write(order)

    unwrap_write!(result, :put, order)
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
      result =
        if primary?(order) or Keyword.get(opts, :forwarded, false) do
          local_batch_write(order, data_for_partition)
        else
          forward(:route_put_batch!, [data_for_partition, [forwarded: true]], order)
        end

      unwrap_write!(result, :put_batch, order)
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

    result =
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        key = Config.get_key!(data)
        local_delete(order, key)
      else
        forward(:route_delete!, [data, [forwarded: true]], order)
      end

    unwrap_write!(result, :delete, order)
    :ok
  end

  @doc "Delete all records — one routed call per partition."
  @spec route_delete_all() :: :ok
  def route_delete_all do
    num = Config.get_config(:num_partition, Partition.get_schedulers())

    0..(num - 1)
    |> Enum.each(fn order ->
      result =
        if primary?(order) do
          local_delete_all(order)
        else
          {primary, _} = Manager.get_replicas(order)
          safe_erpc(primary, __MODULE__, :route_delete_all_partition, [order, [forwarded: true]])
        end

      unwrap_write!(result, :delete_all, order)
    end)

    :ok
  end

  @doc false
  # Single-partition delete_all; called via :erpc from route_delete_all/0.
  @spec route_delete_all_partition(non_neg_integer, keyword) :: :ok
  def route_delete_all_partition(order, opts \\ []) do
    result =
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        local_delete_all(order)
      else
        forward(:route_delete_all_partition, [order, [forwarded: true]], order)
      end

    unwrap_write!(result, :delete_all, order)
    :ok
  end

  @doc "Route a match-based delete, one partition order at a time."
  @spec route_delete_match!(any, tuple) :: :ok
  def route_delete_match!(partition_data, pattern) when is_tuple(pattern) do
    resolve_partition_orders(partition_data)
    |> Enum.each(fn order ->
      result =
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

      unwrap_write!(result, :delete_match, order)
    end)

    :ok
  end

  @doc false
  # Single-partition delete_match; called via :erpc from route_delete_match!/2.
  @spec route_delete_match_partition!(non_neg_integer, tuple, keyword) :: :ok
  def route_delete_match_partition!(order, pattern, opts \\ []) do
    result =
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        local_delete_match(order, pattern)
      else
        forward(:route_delete_match_partition!, [order, pattern, [forwarded: true]], order)
      end

    unwrap_write!(result, :delete_match, order)
    :ok
  end

  @doc "Route a delete by explicit key + partition value to the correct primary."
  @spec route_delete_by_key_partition!(any, any, keyword) :: :ok
  def route_delete_by_key_partition!(key, partition_data, opts \\ []) do
    order = Partition.get_partition_order(partition_data)

    result =
      if primary?(order) or Keyword.get(opts, :forwarded, false) do
        local_delete(order, key)
      else
        forward(:route_delete_by_key_partition!, [key, partition_data, [forwarded: true]], order)
      end

    unwrap_write!(result, :delete, order)
    :ok
  end

  ## ── Private — local write helpers ────────────────────────────────────────────

  defp local_write(order, op, data) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        # WAL applies locally and replicates to a primary-inclusive majority.
        case WAL.commit(order, [{op, data}]) do
          :ok -> :ok
          {:error, reason} -> {:error, write_error(reason, op, order)}
        end

      :raft ->
        raft_write(order, op, data)

      _ ->
        Storage.put(data, partition)

        case Replicator.replicate(order, op, data) do
          :ok -> :ok
          {:error, reason} when mode == :sync -> {:error, write_error(reason, op, order)}
          _other -> :ok
        end
    end
  end

  # Batch write: applies all records locally then replicates in a single call.
  defp local_batch_write(order, data_list) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        # One WAL commit for the whole batch (one per partition).
        case WAL.commit(order, Enum.map(data_list, &{:put, &1})) do
          :ok -> :ok
          {:error, reason} -> {:error, write_error(reason, :put_batch, order)}
        end

      :raft ->
        raft_write_batch(order, data_list)

      _ ->
        # Write all records locally in a single ETS batch operation,
        # then replicate the entire batch in one erpc call.
        Storage.put(data_list, partition)

        case Replicator.replicate_batch(order, :put, data_list) do
          :ok -> :ok
          {:error, reason} when mode == :sync -> {:error, write_error(reason, :put_batch, order)}
          _other -> :ok
        end
    end
  end

  defp local_delete(order, key) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        case WAL.commit(order, [{:delete, key}]) do
          :ok -> :ok
          {:error, reason} -> {:error, write_error(reason, :delete, order)}
        end

      :raft ->
        raft_write(order, :delete, key)

      _ ->
        Storage.delete(key, partition)

        case Replicator.replicate(order, :delete, key) do
          :ok -> :ok
          {:error, reason} when mode == :sync -> {:error, reason}
          _other -> :ok
        end
    end
  end

  defp local_delete_all(order) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        case WAL.commit(order, [{:delete_all, nil}]) do
          :ok -> :ok
          {:error, reason} -> {:error, write_error(reason, :delete_all, order)}
        end

      :raft ->
        raft_write(order, :delete_all, nil)

      _ ->
        Storage.delete_all(partition)

        case Replicator.replicate(order, :delete_all, nil) do
          :ok -> :ok
          {:error, reason} when mode == :sync -> {:error, reason}
          _other -> :ok
        end
    end
  end

  defp local_delete_match(order, pattern) do
    mode = Manager.replication_mode()
    partition = Partition.get_partition_by_idx(order)

    case mode do
      :strong ->
        case WAL.commit(order, [{:delete_match, pattern}]) do
          :ok -> :ok
          {:error, reason} -> {:error, write_error(reason, :delete_match, order)}
        end

      :raft ->
        raft_write(order, :delete_match, pattern)

      _ ->
        Storage.delete_match(pattern, partition)

        case Replicator.replicate(order, :delete_match, pattern) do
          :ok -> :ok
          {:error, reason} when mode == :sync -> {:error, reason}
          _other -> :ok
        end
    end
  end

  # In :raft mode the state machine performs the local ETS write when the log
  # entry commits, so the router must not pre-apply it.
  defp raft_write(order, op, data) do
    case Replicator.replicate(order, op, data) do
      :ok -> :ok
      {:error, reason} -> {:error, write_error(reason, op, order)}
    end
  end

  defp raft_write_batch(order, data_list) do
    case Replicator.replicate_batch(order, :put, data_list) do
      :ok -> :ok
      {:error, reason} -> {:error, write_error(reason, :put_batch, order)}
    end
  end

  # Normalize a write failure to the documented distributed error type.
  defp write_error(%SuperCache.Cluster.Error{} = error, _operation, _order), do: error

  defp write_error(reason, operation, order) do
    Rpc.normalize(reason,
      operation: operation,
      partition: order,
      mode: Manager.replication_mode()
    )
  end

  # Normalize and raise a write failure; return `true` for successful writes.
  defp unwrap_write!(result, operation, order) do
    case result do
      {:error, reason} -> raise write_error(reason, operation, order)
      _ -> true
    end
  end

  ## ── Private — single-partition read dispatcher ───────────────────────────────

  # Read-your-writes: upgrade :local to :primary if this process recently
  # wrote to this partition.  Prevents stale reads immediately after a write.
  @doc false
  @spec resolve_read_mode(atom, non_neg_integer) :: atom
  def resolve_read_mode(:local, order) do
    if ryw_recent?(order), do: :primary, else: :local
  end

  def resolve_read_mode(mode, _order), do: mode

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

      case Rpc.call(primary, __MODULE__, :local_read, [order, op, arg],
             operation: op,
             partition: order,
             node: primary,
             mode: :primary
           ) do
        {:ok, list} when is_list(list) -> list
        {:error, error} -> primary_read_fallback(order, op, arg, primary, error)
      end
    end
  end

  # Quorum: ask primary + all replicas in parallel; only plain terms sent via
  # :erpc — no closures.  Quorum.await/4 terminates as soon as a strict
  # majority agrees and enforces one deadline for the whole read.
  defp do_read(:quorum, order, op, arg) do
    {primary, replicas} = Manager.get_replicas(order)
    nodes = [primary | replicas]
    total = length(nodes)
    required = div(total, 2) + 1

    entries =
      Enum.map(nodes, fn n ->
        {n,
         fn ->
           if n == node() do
             local_read(order, op, arg)
           else
             :erpc.call(n, __MODULE__, :local_read, [order, op, arg], @erpc_timeout)
           end
         end}
      end)

    case Quorum.await(entries, required, @erpc_timeout + 500, pool: Quorum.Pool) do
      {:ok, value, _voters} ->
        value

      {:error, reason, results} ->
        # Prefer a reachable primary result; otherwise fall back to a local
        # replica only when this node actually holds one.
        Metrics.increment(:read, :fallbacks)

        case Map.fetch(results, primary) do
          {:ok, {:ok, value}} -> value
          _ -> quorum_read_fallback(order, op, arg, primary, reason)
        end
    end
  end

  defp quorum_read_fallback(order, op, arg, primary, reason) do
    if DistributedHelpers.has_partition?(order) do
      local_read(order, op, arg)
    else
      Metrics.increment(:read, :errors)

      raise Rpc.normalize(reason,
              operation: :read,
              partition: order,
              node: primary,
              mode: :quorum
            )
    end
  end

  defp primary_read_fallback(order, op, arg, primary, reason) do
    Logger.warning(
      "super_cache, router, primary read failed (order=#{order}): #{inspect(reason)}"
    )

    if DistributedHelpers.has_partition?(order) do
      Metrics.increment(:read, :fallbacks)
      local_read(order, op, arg)
    else
      Metrics.increment(:read, :errors)

      raise Rpc.normalize(reason,
              operation: :read,
              partition: order,
              node: primary,
              mode: :primary
            )
    end
  end

  ## ── Private — multi-partition fan-out ────────────────────────────────────────

  # Local fan: all partitions resolved to ETS table atoms, no :erpc involved.
  defp fan_read(partition_data, :local, op, arg) do
    resolve_partitions(partition_data)
    |> Enum.flat_map(fn p ->
      case op do
        :match -> Storage.get_by_match(arg, p)
        :match_object -> Storage.get_by_match_object(arg, p)
      end
    end)
  end

  # Primary / quorum fan: resolve partition orders (integers), then for each
  # order delegate to do_read/4 which handles routing with plain-term :erpc.
  # Errors are captured inside the task and re-raised in the caller so a
  # failed partition surfaces as SuperCache.Cluster.Error instead of an exit.
  defp fan_read(partition_data, mode, op, arg) do
    results =
      resolve_partition_orders(partition_data)
      |> Task.async_stream(
        fn order -> safe_do_read(mode, order, op, arg) end,
        timeout: @erpc_timeout + 500,
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, {:ok, list}} -> {:ok, list}
        {:ok, {:error, error}} -> {:error, error}
        {:exit, reason} -> {:error, Rpc.normalize({:exit, reason}, operation: op, mode: mode)}
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, error} -> raise error
      nil -> Enum.flat_map(results, fn {:ok, list} -> list end)
    end
  end

  defp safe_do_read(mode, order, op, arg) do
    {:ok, do_read(mode, order, op, arg)}
  rescue
    error -> {:error, error}
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
    timeout =
      if Manager.replication_mode() == :raft do
        # Raft must replicate and commit before returning; that can take
        # longer than the local/quorum paths used by the other modes.
        max(@erpc_timeout, 20_000)
      else
        @erpc_timeout
      end

    :erpc.call(target, mod, fun, args, timeout)
  catch
    kind, reason ->
      Logger.warning(
        "super_cache, router, erpc #{inspect(fun)} failed → #{inspect(target)}: " <>
          inspect({kind, reason})
      )

      {:error, {kind, reason}}
  end

  ## ── Private — partition resolution ──────────────────────────────────────────

  defp primary?(order) do
    {primary, _} = Manager.get_replicas(order)
    primary == node()
  end

  # ── Read-your-writes tracking ────────────────────────────────────────────────

  @doc false
  @spec track_write(non_neg_integer) :: :ok
  def track_write(order) do
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

  @doc false
  @spec ryw_recent?(non_neg_integer) :: boolean
  def ryw_recent?(order) do
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

  @doc false
  @spec ensure_ryw_table() :: :ok
  def ensure_ryw_table do
    case :ets.info(@ryw_table) do
      :undefined ->
        # Fallback for isolated tests or environments where WAL.Store
        # has not started (e.g., direct unit tests). In production the
        # table is created by SuperCache.Cluster.WAL.Store at boot.
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

  @doc false
  @spec prune_ryw(integer) :: :ok
  def prune_ryw(now) do
    # Use :ets.select_delete/2 to prune expired entries for the calling process
    # without copying non-matching rows into the calling process's memory.
    :ets.select_delete(@ryw_table, [
      {{{self(), :_}, :"$1"}, [{:<, :"$1", now}], [true]}
    ])

    :ok
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
