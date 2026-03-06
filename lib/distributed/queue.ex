defmodule SuperCache.Distributed.Queue do
  @moduledoc """
  Cluster-aware FIFO queue.

  Structural mutations (enqueue, dequeue, drain) are routed to the primary
  node for the partition that owns `queue_name`.  Reads (`peak`, `count`)
  are served from the local node.

  API is identical to `SuperCache.Queue`.

  ## Example

      alias SuperCache.Distributed.Queue

      SuperCache.Cluster.Bootstrap.start!(...)
      Queue.add("jobs", :task_a)
      Queue.out("jobs")     # => :task_a
      Queue.count("jobs")   # => 0
  """

  alias SuperCache.Cluster.DistributedStore, as: DS
  alias SuperCache.{Storage, Partition}
  require Logger

  ## Public API ────────────────────────────────────────────────────────────────

  @doc "Enqueue `value`. Routed to primary."
  @spec add(any, any) :: true
  def add(queue_name, value) do
    primary = primary_for(queue_name)

    if primary == node() do
      local_queue_in(queue_name, value)
    else
      Logger.debug(fn -> "super_cache, dist.queue #{inspect(queue_name)}, fwd add → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, :local_queue_in, [queue_name, value], 5_000)
    end
  end

  @doc "Dequeue and return the front value. Routed to primary."
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    primary = primary_for(queue_name)

    if primary == node() do
      local_queue_out(queue_name, default)
    else
      Logger.debug(fn -> "super_cache, dist.queue #{inspect(queue_name)}, fwd out → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, :local_queue_out, [queue_name, default], 5_000)
    end
  end

  @doc "Peek at the front value without removing it. Read from local node."
  @spec peak(any, any) :: any
  def peak(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_peak(part, queue_name, default)
  end

  @doc "Return the number of items. Read from local node."
  @spec count(any) :: non_neg_integer
  def count(queue_name) do
    part = Partition.get_partition(queue_name)

    case Storage.get({:queue, :tail, queue_name}, part) do
      [] -> 0
      [{_, tail}] ->
        case Storage.get({:queue, :head, queue_name}, part) do
          []         -> count(queue_name)
          [{_, 0}]   -> 0
          [{_, head}] -> tail - head + 1
        end
    end
  end

  @doc "Drain all items (destructive). Routed to primary."
  @spec get_all(any) :: list
  def get_all(queue_name) do
    primary = primary_for(queue_name)

    if primary == node() do
      local_queue_drain(queue_name)
    else
      :erpc.call(primary, __MODULE__, :local_queue_drain, [queue_name], 10_000)
    end
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ─────────────

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

  ## Private — queue_in ────────────────────────────────────────────────────────

  defp queue_in(partition, queue_name, value) do
    case Storage.take({:queue, :tail, queue_name}, partition) do
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
        lock(partition, queue_name)
        Storage.delete({:queue, :head, queue_name}, partition)
        Storage.put({{:queue, queue_name, 1}, value}, partition)
        Storage.put({{:queue, :head, queue_name}, 1}, partition)
        Storage.put({{:queue, :tail, queue_name}, 1}, partition)
        unlock(partition, queue_name)
        replicate_put(queue_name, {{:queue, queue_name, 1}, value})
        replicate_put(queue_name, {{:queue, :head, queue_name}, 1})
        replicate_put(queue_name, {{:queue, :tail, queue_name}, 1})
        true

      [{_, counter}] ->
        next = counter + 1
        lock(partition, queue_name)
        Storage.put({{:queue, queue_name, next}, value}, partition)
        Storage.put({{:queue, :tail, queue_name}, next}, partition)
        unlock(partition, queue_name)
        replicate_put(queue_name, {{:queue, queue_name, next}, value})
        replicate_put(queue_name, {{:queue, :tail, queue_name}, next})
        true
    end
  end

  ## Private — queue_out ───────────────────────────────────────────────────────

  defp queue_out(partition, queue_name, default) do
    case Storage.take({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); queue_out(partition, queue_name, default)
        end

      [{_, 0}] -> default

      [{_, counter}] ->
        lock(partition, queue_name)

        value =
          case Storage.take({:queue, queue_name, counter}, partition) do
            [] ->
              reset_queue(partition, queue_name)
              default

            [{_, v}] ->
              next = counter + 1
              case Storage.get({:queue, :tail, queue_name}, partition) do
                [{_, tail}] when next > tail ->
                  reset_queue(partition, queue_name)
                _ ->
                  Storage.put({{:queue, :head, queue_name}, next}, partition)
                  replicate_put(queue_name, {{:queue, :head, queue_name}, next})
              end
              replicate_delete(queue_name, {:queue, queue_name, counter})
              v
          end

        unlock(partition, queue_name)
        value
    end
  end

  ## Private — queue_peak ──────────────────────────────────────────────────────

  defp queue_peak(partition, queue_name, default) do
    case Storage.get({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); queue_peak(partition, queue_name, default)
        end
      [{_, 0}] -> default
      [{_, counter}] ->
        case Storage.get({:queue, queue_name, counter}, partition) do
          []        -> default
          [{_, v}]  -> v
        end
    end
  end

  ## Private — queue_drain ─────────────────────────────────────────────────────

  defp queue_drain(partition, queue_name) do
    case Storage.take({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> []
          _  -> :erlang.yield(); queue_drain(partition, queue_name)
        end

      [{_, 0}] -> []

      [{_, first}] ->
        lock(partition, queue_name)
        [{_, last}] = Storage.take({:queue, :tail, queue_name}, partition)

        values =
          Enum.reduce(first..last, [], fn i, acc ->
            case Storage.take({:queue, queue_name, i}, partition) do
              []       -> acc
              [{_, v}] -> [v | acc]
            end
          end)
          |> Enum.reverse()

        reset_queue(partition, queue_name)
        unlock(partition, queue_name)

        # Replicate the reset state to all replicas.
        replicate_put(queue_name, {{:queue, :head, queue_name}, 0})
        replicate_put(queue_name, {{:queue, :tail, queue_name}, 0})
        for i <- first..last do
          replicate_delete(queue_name, {:queue, queue_name, i})
        end

        values
    end
  end

  ## Private — init / lock / reset ─────────────────────────────────────────────

  defp queue_init(queue_name) do
    partition = Partition.get_partition(queue_name)
    if Storage.insert_new({{:queue, :updating, queue_name}, true}, partition) do
      Storage.put({{:queue, :head, queue_name}, 0}, partition)
      Storage.put({{:queue, :tail, queue_name}, 0}, partition)
      Storage.delete({:queue, :updating, queue_name}, partition)
      replicate_put(queue_name, {{:queue, :head, queue_name}, 0})
      replicate_put(queue_name, {{:queue, :tail, queue_name}, 0})
    end
  end

  defp lock(partition, queue_name),   do: Storage.put({{:queue, :updating, queue_name}, true}, partition)
  defp unlock(partition, queue_name), do: Storage.delete({:queue, :updating, queue_name}, partition)

  defp reset_queue(partition, queue_name) do
    Storage.put({{:queue, :head, queue_name}, 0}, partition)
    Storage.put({{:queue, :tail, queue_name}, 0}, partition)
  end

  ## Private — replication helpers ─────────────────────────────────────────────

  # Queue data lives in a single partition keyed by queue_name.
  # We call the Replicator directly with the partition index.
  defp partition_idx(queue_name) do
    Partition.get_partition_order(queue_name)
  end

  defp replicate_put(queue_name, record) do
    SuperCache.Cluster.Replicator.replicate(partition_idx(queue_name), :put, record)
  end

  defp replicate_delete(queue_name, key) do
    SuperCache.Cluster.Replicator.replicate(partition_idx(queue_name), :delete, key)
  end

  defp primary_for(queue_name) do
    idx = Partition.get_partition_order(queue_name)
    {primary, _} = SuperCache.Cluster.Manager.get_replicas(idx)
    primary
  end
end
