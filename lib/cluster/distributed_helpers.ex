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

  Quorum reads use `Task.async` with early termination — as soon as a majority
  of replicas agree on a result, remaining tasks are killed. This is more
  efficient than `Task.async_stream` which always waits for all tasks to
  complete before computing consensus.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Config}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

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
        ]) :: :ok | {:error, term()}
  def apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("super_cache, distributed_helpers, 3pc failed: #{inspect(reason)}")
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

  The return value of the called function, or raises on `:erpc` failure.
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

      :erpc.call(primary, caller, fun, args, 5_000)
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
  Otherwise, an `:erpc.call` is made to the primary node.
  """
  @spec read_primary(module(), atom(), [term()], non_neg_integer()) :: term()
  def read_primary(caller, fun, args, partition_idx) do
    {primary, _} = Manager.get_replicas(partition_idx)

    if primary == node(),
      do: apply(caller, fun, args),
      else: :erpc.call(primary, caller, fun, args, 5_000)
  end

  # ── Quorum read ──────────────────────────────────────────────────────────────

  @doc """
  Reads from all replicas and returns the result that achieves quorum (majority).

  Uses `Task.async` with early termination: as soon as a majority of replicas
  agree on a result, remaining tasks are killed immediately. This avoids waiting
  for slow replicas once quorum is satisfied.

  If no result reaches quorum, falls back to the primary node's result.
  """
  @spec read_quorum(module(), atom(), [term()], non_neg_integer()) :: term()
  def read_quorum(caller, fun, args, partition_idx) do
    {primary, replicas} = Manager.get_replicas(partition_idx)
    nodes = [primary | replicas]
    total = length(nodes)
    required = div(total, 2) + 1

    tasks =
      Enum.map(nodes, fn n ->
        Task.async(fn ->
          if n == node(),
            do: apply(caller, fun, args),
            else: :erpc.call(n, caller, fun, args, 5_000)
        end)
      end)

    await_quorum(tasks, required, %{}, caller, fun, args, primary)
  end

  # ── Private — quorum await ───────────────────────────────────────────────────

  # No tasks left — return the most frequent result, or fall back to primary.
  defp await_quorum([], _required, counts, caller, fun, args, primary) do
    case map_max(counts) do
      {result, _count} ->
        result

      nil ->
        if primary == node(),
          do: apply(caller, fun, args),
          else: :erpc.call(primary, caller, fun, args, 5_000)
    end
  end

  defp await_quorum([task | rest], required, counts, caller, fun, args, primary) do
    result =
      case Task.yield(task, 5_000) || Task.shutdown(task, :brutal_kill) do
        {:ok, val} -> val
        _ -> nil
      end

    new_counts =
      if result != nil do
        Map.update(counts, result, 1, &(&1 + 1))
      else
        counts
      end

    # Check if any result has reached quorum — early termination.
    if quorum_reached?(new_counts, required) do
      Enum.each(rest, &Task.shutdown(&1, :brutal_kill))
      {winner, _} = Enum.max_by(new_counts, fn {_, c} -> c end)
      winner
    else
      await_quorum(rest, required, new_counts, caller, fun, args, primary)
    end
  end

  # Returns {result, count} for the most frequent result, or nil if map is empty.
  defp map_max(counts) when map_size(counts) == 0, do: nil
  defp map_max(counts), do: Enum.max_by(counts, fn {_, c} -> c end)

  # True if any result has reached the required quorum count.
  defp quorum_reached?(counts, required) do
    Enum.any?(counts, fn {_, count} -> count >= required end)
  end
end
