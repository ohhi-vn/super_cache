defmodule SuperCache.Cluster.DistributedHelpers do
  @moduledoc """
  Shared helpers for distributed read/write operations.

  Extracts common patterns used across `SuperCache.KeyValue`, `SuperCache.Queue`,
  `SuperCache.Stack`, and `SuperCache.Struct` for:

  - Determining cluster mode (`distributed?/0`)
  - Applying writes with replication (`apply_write/3`)
  - Routing writes to the primary node (`route_write/4`)
  - Routing reads with `:local`/`:primary`/`:quorum` modes (`route_read/5`)
  - Checking local partition ownership (`has_partition?/1`)
  - Reading from the primary node (`read_primary/4`)
  - Reading with quorum consensus (`read_quorum/4`)

  ## Quorum reads

  Quorum reads use `SuperCache.Cluster.Quorum.await/4` with selective
  receive and a single deadline. As soon as a strict majority agrees on a
  result, remaining tasks are killed. This is more efficient than
  `Task.async_stream`, which always waits for all tasks to complete before
  computing consensus, and it counts `nil` as a valid value.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Cluster.{Manager, Metrics, Quorum, Raft, Replicator, Rpc, WAL}
  alias SuperCache.{Config, Storage}

  # Inline hot-path cluster-mode check so callers don't pay a call overhead.
  @compile {:inline, distributed?: 0}

  # ── Cluster mode ─────────────────────────────────────────────────────────────

  @doc """
  Returns `true` when the cluster is running in distributed mode.

  Inlined by the compiler for hot-path performance.
  """
  @spec distributed?() :: boolean()
  def distributed?, do: Config.distributed?()

  # ── Write operations ─────────────────────────────────────────────────────────

  @doc """
  Applies a batch of write operations with the configured replication strategy.

  - **Strong replication** (`:strong`): delegates to `ThreePhaseCommit.commit/2`
    for atomic distributed commits via the WAL-based 3PC protocol.
  - **Async / sync replication**: applies each operation to local storage first,
    then replicates to replica nodes via `Replicator`.

  ## Operations

  Each operation is a tuple:

  | Operation                   | Description                          |
  |-----------------------------|--------------------------------------|
  | `{:put, record}`           | Insert or update a record            |
  | `{:delete, key}`           | Delete a record by key               |
  | `{:delete_match, pattern}` | Delete records matching a pattern    |
  | `{:delete_all, _}`         | Delete all records in the partition  |

  ## Returns

  - `:ok` on success
  - `{:error, reason}` when 3PC commit fails (strong mode only)
  """
  @spec apply_write(non_neg_integer(), term(), [
          {:put, term()}
          | {:delete, term()}
          | {:delete_match, term()}
          | {:delete_all, term()}
          | {:increment, term(), term(), term()}
        ]) :: :ok | {:error, SuperCache.Cluster.Error.t()}
  def apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case WAL.commit(idx, ops) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("super_cache, distributed_helpers, wal failed: #{inspect(reason)}")
            {:error, normalize_error(reason, idx, :strong)}
        end

      :raft ->
        # The Raft state machine is the only place that writes to ETS in this
        # mode, so no local pre-apply is performed here.
        case Raft.submit(idx, {:batch, raft_commands(ops)}, operation: {__MODULE__, :apply_write}) do
          {:ok, _reply} -> :ok
          {:error, reason} -> {:error, normalize_error(reason, idx, :raft)}
        end

      mode ->
        Enum.reduce_while(ops, :ok, fn op, _acc ->
          replicate_op_result(op, partition, idx, mode)
        end)
    end
  end

  defp raft_commands(ops) do
    Enum.map(ops, fn
      {:increment, key, default, step} -> {:increment, {key, default, step}}
      {op, arg} -> {op, arg}
    end)
  end

  defp replicate_op_result(op, partition, idx, mode) do
    applied = local_apply(op, partition)
    {op_name, op_arg} = replicate_op(op, applied)

    case Replicator.replicate(idx, op_name, op_arg) do
      :ok ->
        {:cont, :ok}

      {:error, reason} when mode == :sync ->
        {:halt, {:error, normalize_error(reason, idx, mode)}}

      _other ->
        {:cont, :ok}
    end
  end

  defp local_apply({:put, record}, partition), do: Storage.put(record, partition)
  defp local_apply({:delete, key}, partition), do: Storage.delete(key, partition)

  defp local_apply({:delete_match, pattern}, partition),
    do: Storage.delete_match(pattern, partition)

  defp local_apply({:delete_all, _}, partition), do: Storage.delete_all(partition)

  defp local_apply({:increment, key, default, step}, partition),
    do: Storage.update_counter(key, partition, {2, step}, {key, default})

  defp replicate_op({:put, record}, _applied), do: {:put, record}
  defp replicate_op({:delete, key}, _applied), do: {:delete, key}
  defp replicate_op({:delete_match, pattern}, _applied), do: {:delete_match, pattern}
  defp replicate_op({:delete_all, _}, _applied), do: {:delete_all, nil}

  defp replicate_op({:increment, key, _default, _step}, new_value),
    do: {:put, {key, new_value}}

  defp normalize_error(%SuperCache.Cluster.Error{} = error, _idx, _mode), do: error

  defp normalize_error(reason, idx, mode) do
    Rpc.normalize(reason,
      operation: :apply_write,
      partition: idx,
      mode: mode
    )
  end

  # ── Write routing ────────────────────────────────────────────────────────────

  @doc """
  Routes a write operation to the primary replica for the given partition.

  If the current node is the primary, the function is applied locally.
  Otherwise, an `:erpc.call` is made to the primary node.

  ## Parameters

  - `caller`        — the module that owns the function being called (replaces `__MODULE__`)
  - `fun`           — the function name (atom)
  - `args`          — the function arguments (list)
  - `partition_idx` — the partition index used to look up the primary node

  ## Returns

  The return value of the called function. Transport failures raise
  `SuperCache.Cluster.Error` rather than leaking a raw `:erpc` exit.
  """
  @spec route_write(module(), atom(), [term()], non_neg_integer()) :: term()
  def route_write(caller, fun, args, partition_idx) do
    {primary, _} = Manager.get_replicas(partition_idx)

    if primary == node() do
      apply(caller, fun, args)
    else
      SuperCache.Log.debug(fn ->
        "super_cache, fwd #{fun} → #{inspect(primary)}"
      end)

      Rpc.call!(primary, caller, fun, args,
        operation: {caller, fun},
        partition: partition_idx,
        node: primary,
        mode: Manager.replication_mode()
      )
    end
  end

  # ── Read routing ─────────────────────────────────────────────────────────────

  @doc """
  Routes a read operation according to the requested read mode.

  ## Read modes

  - `:local`   — read from the local node. If the local node does **not** hold a
    replica, automatically escalates to `:primary`.
  - `:primary` — read from the primary replica.
  - `:quorum`  — read from all replicas and return the majority result.

  ## Parameters

  - `caller`        — the module that owns the function being called
  - `fun`           — the function name (atom)
  - `args`          — the function arguments (list)
  - `partition_idx` — the partition index
  - `opts`          — keyword list, supports `:read_mode` (default `:local`)

  ## Returns

  The return value of the called function.
  """
  @spec route_read(module(), atom(), [term()], non_neg_integer(), keyword()) :: term()
  def route_read(caller, fun, args, partition_idx, opts) do
    mode = Keyword.get(opts, :read_mode, :local)
    effective = if mode == :local and not has_partition?(partition_idx), do: :primary, else: mode

    case effective do
      :local -> apply(caller, fun, args)
      :primary -> read_primary(caller, fun, args, partition_idx)
      :quorum -> read_quorum(caller, fun, args, partition_idx)
    end
  end

  # ── Partition check ──────────────────────────────────────────────────────────

  @doc """
  Run a read function, converting a distributed read failure into
  `{:error, exception}` instead of raising.

  Non-bang read APIs use this wrapper so a failed `:primary` or `:quorum`
  read with no usable local copy returns the documented error tuple. Local
  errors that are not distributed failures still propagate unchanged.
  """
  @spec read_or_error((-> term())) :: term() | {:error, SuperCache.Cluster.Error.t()}
  def read_or_error(fun) when is_function(fun, 0) do
    fun.()
  rescue
    error in SuperCache.Cluster.Error -> {:error, error}
  end

  @doc """
  Returns `true` if the current node holds a replica (primary or secondary)
  for the given partition index.
  """
  @spec has_partition?(non_neg_integer()) :: boolean()
  def has_partition?(partition_idx) do
    {primary, replicas} = Manager.get_replicas(partition_idx)
    node() in [primary | replicas]
  end

  # ── Primary read ─────────────────────────────────────────────────────────────

  @doc """
  Reads from the primary replica for the given partition.

  If the current node is the primary, the function is applied locally.
  Otherwise an `:erpc.call` is made to the primary node. When the primary
  cannot be reached, the read **falls back to the local replica** (stale
  data allowed) with a warning rather than crashing the caller — reads
  follow eventual-consistency semantics.
  """
  @spec read_primary(module(), atom(), [term()], non_neg_integer()) :: term()
  def read_primary(caller, fun, args, partition_idx) do
    {primary, _} = Manager.get_replicas(partition_idx)

    if primary == node() do
      apply(caller, fun, args)
    else
      case Rpc.call(primary, caller, fun, args,
             operation: {caller, fun},
             partition: partition_idx,
             node: primary,
             mode: :primary
           ) do
        {:ok, value} ->
          value

        {:error, error} ->
          primary_read_failed(error, caller, fun, args, partition_idx, primary)
      end
    end
  end

  defp primary_read_failed(error, caller, fun, args, partition_idx, primary) do
    Logger.warning(
      "super_cache, distributed_helpers, primary read failed " <>
        "(partition=#{partition_idx}, primary=#{inspect(primary)}): " <>
        inspect(error)
    )

    if has_partition?(partition_idx) do
      Metrics.increment(:read, :fallbacks)
      apply(caller, fun, args)
    else
      Metrics.increment(:read, :errors)
      raise error
    end
  end

  # ── Quorum read ──────────────────────────────────────────────────────────────

  @doc """
  Reads from all replicas and returns the result that achieves quorum (majority).

  Uses `Quorum.await/4` with early termination: as soon as a strict majority
  of nodes agree on a result, remaining tasks are killed immediately.  `nil`
  is a valid value and can win a quorum.

  If no result reaches quorum, falls back to the primary node's result.
  """
  @spec read_quorum(module(), atom(), [term()], non_neg_integer()) :: term()
  def read_quorum(caller, fun, args, partition_idx) do
    {primary, replicas} = Manager.get_replicas(partition_idx)
    nodes = [primary | replicas]
    total = length(nodes)
    required = div(total, 2) + 1

    entries =
      Enum.map(nodes, fn n ->
        {n,
         fn ->
           if n == node() do
             apply(caller, fun, args)
           else
             :erpc.call(n, caller, fun, args, 5_000)
           end
         end}
      end)

    case Quorum.await(entries, required, 5_500, pool: Quorum.Pool) do
      {:ok, value, _voters} ->
        value

      {:error, reason, results} ->
        Metrics.increment(:read, :fallbacks)

        case Map.fetch(results, primary) do
          {:ok, {:ok, value}} -> value
          _ -> quorum_read_fallback(caller, fun, args, partition_idx, primary, reason)
        end
    end
  end

  defp quorum_read_fallback(caller, fun, args, partition_idx, primary, reason) do
    if has_partition?(partition_idx) do
      apply(caller, fun, args)
    else
      Metrics.increment(:read, :errors)

      raise Rpc.normalize(reason,
              operation: fun,
              partition: partition_idx,
              node: primary,
              mode: :quorum
            )
    end
  end
end

## ── Private ──────────────────────────────────────────────────────────────────
