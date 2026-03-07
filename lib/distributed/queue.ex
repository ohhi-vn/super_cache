defmodule SuperCache.Distributed.Queue do
  @moduledoc """
  Cluster-aware FIFO queue.

  Structural mutations (enqueue, dequeue, drain) are routed to the primary
  node for the partition that owns `queue_name`.  Reads (`peak`, `count`)
  are served from the local node by default and support `:read_mode`.

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
  must NOT write to local Storage before calling `apply_write/3`.  State
  is therefore read non-destructively (`Storage.get` instead of
  `Storage.take`), ops are collected, and then `apply_write/3` atomically
  applies them everywhere.  Lock keys (`{:queue, :updating, …}`) are
  coordinator-only primitives and are never included in replicated ops.
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}
  require Logger

  ## Public API ────────────────────────────────────────────────────────────────

  @doc "Enqueue `value`. Routed to primary."
  @spec add(any, any) :: true
  def add(queue_name, value) do
    route(queue_name, :local_queue_in, [queue_name, value])
  end

  @doc "Dequeue and return the front value. Routed to primary."
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    route(queue_name, :local_queue_out, [queue_name, default])
  end

  @doc """
  Peek at the front value without removing it.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec peak(any, any, keyword) :: any
  def peak(queue_name, default \\ nil, opts \\ []) do
    route_read(queue_name, :local_queue_peak, [queue_name, default], opts)
  end

  @doc """
  Return the number of items.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec count(any, keyword) :: non_neg_integer
  def count(queue_name, opts \\ []) do
    route_read(queue_name, :local_queue_count, [queue_name], opts)
  end

  @doc "Drain all items (destructive). Routed to primary."
  @spec get_all(any) :: list
  def get_all(queue_name) do
    route(queue_name, :local_queue_drain, [queue_name])
  end

  ## Remote entry points — writes ───────────────────────────────────────────────

  @doc false
  def local_queue_in(queue_name, value) do
    part = Partition.get_partition(queue_name)
    queue_in(part, queue_name, value)
  end

  @doc false
  def local_queue_out(queue_name, default) do
    part = Partition.get_partition(queue_name)
    queue_out(part, queue_name, default)
  end

  @doc false
  def local_queue_drain(queue_name) do
    part = Partition.get_partition(queue_name)
    queue_drain(part, queue_name)
  end

  ## Remote entry points — reads ────────────────────────────────────────────────

  @doc false
  def local_queue_peak(queue_name, default) do
    part = Partition.get_partition(queue_name)
    queue_peak(part, queue_name, default)
  end

  @doc false
  def local_queue_count(queue_name) do
    part = Partition.get_partition(queue_name)
    count_safe(part, queue_name, 50)
  end

  ## Private — count_safe ──────────────────────────────────────────────────────

  defp count_safe(_part, _queue_name, 0), do: 0

  defp count_safe(part, queue_name, retries) do
    case Storage.get({:queue, :updating, queue_name}, part) do
      [_] ->
        :erlang.yield()
        count_safe(part, queue_name, retries - 1)

      [] ->
        tail = Storage.get({:queue, :tail, queue_name}, part)
        head = Storage.get({:queue, :head, queue_name}, part)

        case {head, tail} do
          {[], []} ->
            0

          {[{_, 0}], _} ->
            0

          {_, [{_, 0}]} ->
            0

          {[], _} ->
            :erlang.yield()
            count_safe(part, queue_name, retries - 1)

          {[{_, h}], [{_, t}]} ->
            max(0, t - h + 1)
        end
    end
  end

  ## Private — queue_in ────────────────────────────────────────────────────────
  #
  # Reads current tail with non-destructive Storage.get so that apply_write/3
  # in :strong mode can apply ops (including the tail update) atomically via
  # ThreePhaseCommit, which calls apply_local last on the primary.

  defp queue_in(partition, queue_name, value) do
    case Storage.get({:queue, :tail, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] ->
            queue_init(queue_name)
            queue_in(partition, queue_name, value)

          _ ->
            :erlang.yield()
            queue_in(partition, queue_name, value)
        end

      [{_, 0}] ->
        # First item after a reset — (re-)establish head and tail at 1.
        lock(partition, queue_name)

        ops = [
          {:delete, {:queue, :head, queue_name}},
          {:put, {{:queue, queue_name, 1}, value}},
          {:put, {{:queue, :head, queue_name}, 1}},
          {:put, {{:queue, :tail, queue_name}, 1}}
        ]

        apply_write(partition_idx(queue_name), partition, ops)
        unlock(partition, queue_name)
        true

      [{_, counter}] ->
        next = counter + 1
        lock(partition, queue_name)

        ops = [
          {:put, {{:queue, queue_name, next}, value}},
          {:put, {{:queue, :tail, queue_name}, next}}
        ]

        apply_write(partition_idx(queue_name), partition, ops)
        unlock(partition, queue_name)
        true
    end
  end

  ## Private — queue_out ───────────────────────────────────────────────────────

  defp queue_out(partition, queue_name, default) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] ->
        :erlang.yield()
        queue_out(partition, queue_name, default)

      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          [] ->
            default

          [{_, 0}] ->
            default

          [{_, counter}] ->
            lock(partition, queue_name)
            idx = partition_idx(queue_name)

            {value, ops} =
              case Storage.get({:queue, queue_name, counter}, partition) do
                [] ->
                  # Item missing — reset the queue to empty.
                  ops = [
                    {:put, {{:queue, :head, queue_name}, 0}},
                    {:put, {{:queue, :tail, queue_name}, 0}}
                  ]

                  {default, ops}

                [{_, v}] ->
                  next = counter + 1

                  ops =
                    case Storage.get({:queue, :tail, queue_name}, partition) do
                      [{_, tail}] when next > tail ->
                        # Dequeued last item — reset to empty.
                        [
                          {:delete, {:queue, queue_name, counter}},
                          {:put, {{:queue, :head, queue_name}, 0}},
                          {:put, {{:queue, :tail, queue_name}, 0}}
                        ]

                      _ ->
                        [
                          {:delete, {:queue, queue_name, counter}},
                          {:put, {{:queue, :head, queue_name}, next}}
                        ]
                    end

                  {v, ops}
              end

            apply_write(idx, partition, ops)
            unlock(partition, queue_name)

            Logger.debug(fn ->
              "super_cache, dist.queue #{inspect(queue_name)}, out: #{inspect(value)}"
            end)

            value
        end
    end
  end

  ## Private — queue_peak ──────────────────────────────────────────────────────

  defp queue_peak(partition, queue_name, default) do
    case Storage.get({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] ->
            default

          _ ->
            :erlang.yield()
            queue_peak(partition, queue_name, default)
        end

      [{_, 0}] ->
        default

      [{_, counter}] ->
        case Storage.get({:queue, queue_name, counter}, partition) do
          [] -> default
          [{_, v}] -> v
        end
    end
  end

  ## Private — queue_drain ─────────────────────────────────────────────────────

  defp queue_drain(partition, queue_name) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] ->
        :erlang.yield()
        queue_drain(partition, queue_name)

      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          [] ->
            []

          [{_, 0}] ->
            []

          [{_, first}] ->
            lock(partition, queue_name)
            idx = partition_idx(queue_name)
            [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)

            # Collect values + build delete ops; reads are non-destructive.
            {values, delete_ops} =
              Enum.reduce(first..last, {[], []}, fn i, {vals, ops} ->
                case Storage.get({:queue, queue_name, i}, partition) do
                  [] -> {vals, ops}
                  [{_, v}] -> {[v | vals], [{:delete, {:queue, queue_name, i}} | ops]}
                end
              end)

            reset_ops = [
              {:put, {{:queue, :head, queue_name}, 0}},
              {:put, {{:queue, :tail, queue_name}, 0}}
            ]

            apply_write(idx, partition, delete_ops ++ reset_ops)
            unlock(partition, queue_name)

            Logger.debug(fn ->
              "super_cache, dist.queue #{inspect(queue_name)}, drained #{length(values)} item(s)"
            end)

            Enum.reverse(values)
        end
    end
  end

  ## Private — init / lock / reset ─────────────────────────────────────────────

  defp queue_init(queue_name) do
    partition = Partition.get_partition(queue_name)

    if Storage.insert_new({{:queue, :updating, queue_name}, true}, partition) do
      idx = partition_idx(queue_name)

      ops = [
        {:put, {{:queue, :head, queue_name}, 0}},
        {:put, {{:queue, :tail, queue_name}, 0}}
      ]

      apply_write(idx, partition, ops)
      Storage.delete({:queue, :updating, queue_name}, partition)
    end
  end

  defp lock(partition, queue_name) do
    Storage.put({{:queue, :updating, queue_name}, true}, partition)
  end

  defp unlock(partition, queue_name) do
    Storage.delete({:queue, :updating, queue_name}, partition)
  end

  ## Private — routing ──────────────────────────────────────────────────────────

  defp route(queue_name, fun, args) do
    {primary, _} = Manager.get_replicas(partition_idx(queue_name))

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.queue #{inspect(queue_name)}, fwd #{fun} → #{inspect(primary)}"
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
            Logger.error("super_cache, dist.queue, 3pc failed: #{inspect(reason)}")
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

  defp partition_idx(queue_name), do: Partition.get_partition_order(queue_name)

  ## Private — read routing ─────────────────────────────────────────────────────

  defp route_read(queue_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)

    effective_mode =
      if mode == :local and not has_partition?(queue_name), do: :primary, else: mode

    case effective_mode do
      :local -> apply(__MODULE__, fun, args)
      :primary -> route_read_primary(queue_name, fun, args)
      :quorum -> route_read_quorum(queue_name, fun, args)
    end
  end

  defp has_partition?(queue_name) do
    idx = partition_idx(queue_name)
    {primary, replicas} = Manager.get_replicas(idx)
    node() in [primary | replicas]
  end

  defp route_read_primary(queue_name, fun, args) do
    {primary, _} = Manager.get_replicas(partition_idx(queue_name))

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.queue #{inspect(queue_name)}, read_primary #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read_quorum(queue_name, fun, args) do
    {primary, replicas} = Manager.get_replicas(partition_idx(queue_name))

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
