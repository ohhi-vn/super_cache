defmodule SuperCache.Distributed.Stack do
  @moduledoc """
  Cluster-aware LIFO stack.

  Structural mutations (`push`, `pop`, `get_all`) are routed to the primary
  node for the partition that owns `stack_name`.  Reads (`count`) are served
  from the local node by default and support `:read_mode`.

  Write replication mode is controlled by the cluster-wide `:replication_mode`
  setting (see `SuperCache.Cluster.Bootstrap`):

  | Mode      | Guarantee              | Extra latency    |
  |-----------|------------------------|------------------|
  | `:async`  | Eventual (default)     | None             |
  | `:sync`   | At-least-once delivery | +1 RTT per write |
  | `:strong` | Three-phase commit     | +3 RTTs per write|

  ## Design note — 3PC and local-state ordering

  In `:strong` mode, `ThreePhaseCommit.commit/2` applies all ops to the
  primary last (after replicas have acknowledged), so mutating functions
  must NOT write to local Storage before calling `apply_write/3`.  The
  counter is therefore read non-destructively (`Storage.get` instead of
  `Storage.take`), ops are collected into a list, and `apply_write/3`
  atomically applies them everywhere.  The `:updating` lock key is a
  coordinator-only primitive and is never included in replicated ops.
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}
  require Logger

  ## Public API ────────────────────────────────────────────────────────────────

  @doc "Push `value` onto `stack_name`. Routed to primary."
  @spec push(any, any) :: true
  def push(stack_name, value) do
    route(stack_name, :local_push, [stack_name, value])
  end

  @doc "Pop and return the top value. Routed to primary."
  @spec pop(any, any) :: any
  def pop(stack_name, default \\ nil) do
    route(stack_name, :local_pop, [stack_name, default])
  end

  @doc """
  Return the number of items.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec count(any, keyword) :: non_neg_integer
  def count(stack_name, opts \\ []) do
    route_read(stack_name, :local_count, [stack_name], opts)
  end

  @doc "Return all items top-first and clear the stack. Routed to primary."
  @spec get_all(any) :: list
  def get_all(stack_name) do
    route(stack_name, :local_get_all, [stack_name])
  end

  ## Remote entry points — writes ───────────────────────────────────────────────

  @doc false
  def local_push(stack_name, value) do
    part = Partition.get_partition(stack_name)
    stack_push(part, stack_name, value)
  end

  @doc false
  def local_pop(stack_name, default) do
    part = Partition.get_partition(stack_name)
    stack_pop(part, stack_name, default)
  end

  @doc false
  def local_get_all(stack_name) do
    part = Partition.get_partition(stack_name)
    stack_drain(part, stack_name)
  end

  ## Remote entry points — reads ────────────────────────────────────────────────

  @doc false
  def local_count(stack_name) do
    part = Partition.get_partition(stack_name)

    case Storage.get({:stack, :counter, stack_name}, part) do
      [] -> 0
      [{_, count}] -> count
    end
  end

  ## Private — push ────────────────────────────────────────────────────────────
  #
  # Reads the counter with non-destructive Storage.get so that apply_write/3
  # in :strong mode can hand all ops to ThreePhaseCommit, which applies them
  # to the primary last (after replicas ACK).

  defp stack_push(partition, stack_name, value) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] ->
            stack_init(stack_name)
            stack_push(partition, stack_name, value)

          _ ->
            :erlang.yield()
            stack_push(partition, stack_name, value)
        end

      [{_, counter}] ->
        next = counter + 1
        idx = partition_idx(stack_name)

        lock(partition, stack_name)

        ops = [
          {:put, {{:stack, :counter, stack_name}, next}},
          {:put, {{:stack, stack_name, next}, value}}
        ]

        apply_write(idx, partition, ops)
        unlock(partition, stack_name)
        true
    end
  end

  ## Private — pop ─────────────────────────────────────────────────────────────

  defp stack_pop(partition, stack_name, default) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] ->
            default

          _ ->
            :erlang.yield()
            stack_pop(partition, stack_name, default)
        end

      [{_, 0}] ->
        default

      [{_, counter}] ->
        lock(partition, stack_name)
        idx = partition_idx(stack_name)
        next = counter - 1

        {value, ops} =
          case Storage.get({:stack, stack_name, counter}, partition) do
            [] ->
              # Counter/item mismatch — reset to empty.
              ops = [{:put, {{:stack, :counter, stack_name}, 0}}]
              {default, ops}

            [{_, v}] ->
              ops = [
                {:delete, {:stack, stack_name, counter}},
                {:put, {{:stack, :counter, stack_name}, next}}
              ]

              {v, ops}
          end

        apply_write(idx, partition, ops)
        unlock(partition, stack_name)
        value
    end
  end

  ## Private — drain (destructive, top-first) ──────────────────────────────────

  defp stack_drain(partition, stack_name) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] ->
            []

          _ ->
            :erlang.yield()
            stack_drain(partition, stack_name)
        end

      [{_, 0}] ->
        []

      [{_, counter}] ->
        lock(partition, stack_name)
        idx = partition_idx(stack_name)

        # Traverse bottom→top (1..counter), prepending each value.
        # Prepending during an ascending traversal produces a top-first list
        # [counter, counter-1, …, 1] without a separate Enum.reverse/1 call.
        {values, delete_ops} =
          Enum.reduce(1..counter, {[], []}, fn x, {vals, ops} ->
            case Storage.get({:stack, stack_name, x}, partition) do
              [] -> {vals, ops}
              [{_, v}] -> {[v | vals], [{:delete, {:stack, stack_name, x}} | ops]}
            end
          end)

        reset_ops = [{:put, {{:stack, :counter, stack_name}, 0}}]

        apply_write(idx, partition, delete_ops ++ reset_ops)
        unlock(partition, stack_name)

        values
    end
  end

  ## Private — init / lock ──────────────────────────────────────────────────────

  defp stack_init(stack_name) do
    partition = Partition.get_partition(stack_name)
    idx = partition_idx(stack_name)
    apply_write(idx, partition, [{:put, {{:stack, :counter, stack_name}, 0}}])
  end

  defp lock(partition, stack_name) do
    Storage.put({{:stack, :updating, stack_name}, true}, partition)
  end

  defp unlock(partition, stack_name) do
    Storage.delete({:stack, :updating, stack_name}, partition)
  end

  ## Private — routing ──────────────────────────────────────────────────────────

  defp route(stack_name, fun, args) do
    {primary, _} = Manager.get_replicas(partition_idx(stack_name))

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.stack #{inspect(stack_name)}, fwd #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # Dispatch to 3PC or async/sync replication depending on replication_mode.
  # In :strong mode ThreePhaseCommit.commit/2 handles local apply + replication.
  # In other modes, Storage is written locally first then replicated.
  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("super_cache, dist.stack, 3pc failed: #{inspect(reason)}")
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

  defp partition_idx(stack_name), do: Partition.get_partition_order(stack_name)

  ## Private — read routing ─────────────────────────────────────────────────────

  defp route_read(stack_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)

    effective_mode =
      if mode == :local and not has_partition?(stack_name), do: :primary, else: mode

    case effective_mode do
      :local -> apply(__MODULE__, fun, args)
      :primary -> route_read_primary(stack_name, fun, args)
      :quorum -> route_read_quorum(stack_name, fun, args)
    end
  end

  defp has_partition?(stack_name) do
    idx = partition_idx(stack_name)
    {primary, replicas} = Manager.get_replicas(idx)
    node() in [primary | replicas]
  end

  defp route_read_primary(stack_name, fun, args) do
    {primary, _} = Manager.get_replicas(partition_idx(stack_name))

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.stack #{inspect(stack_name)}, read_primary #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read_quorum(stack_name, fun, args) do
    {primary, replicas} = Manager.get_replicas(partition_idx(stack_name))

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

    majority = div(length(results), 2) + 1

    case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
      {result, _} ->
        result

      nil ->
        if primary == node(),
          do: apply(__MODULE__, fun, args),
          else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
