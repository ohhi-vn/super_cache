defmodule SuperCache.Distributed.Stack do
  @moduledoc """
  Cluster-aware LIFO stack.

  Structural mutations (`push`, `pop`, `get_all`) are routed to the primary
  node for the partition that owns `stack_name`. Reads (`count`) are served
  from the local node.

  API is identical to `SuperCache.Stack`.

  ## Example

      alias SuperCache.Distributed.Stack

      SuperCache.Cluster.Bootstrap.start!(...)
      Stack.push("work", :item_a)
      Stack.push("work", :item_b)
      Stack.pop("work")    # => :item_b
      Stack.count("work")  # => 1
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
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

  @doc "Return the number of items. Read from local node."
  @spec count(any) :: non_neg_integer
  def count(stack_name) do
    part = Partition.get_partition(stack_name)
    case Storage.get({:stack, :counter, stack_name}, part) do
      []           -> 0
      [{_, count}] -> count
    end
  end

  @doc "Return all items top-first and clear the stack. Routed to primary."
  @spec get_all(any) :: list
  def get_all(stack_name) do
    route(stack_name, :local_get_all, [stack_name])
  end

  ## Remote entry points (called via :erpc — do NOT call directly) ─────────────

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
    to_list(part, stack_name)
  end

  ## Private — push ────────────────────────────────────────────────────────────

  defp stack_push(partition, stack_name, value) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
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
        Storage.put({{:stack, :updating, stack_name}, true}, partition)
        Storage.put({{:stack, :counter, stack_name}, next}, partition)
        Storage.put({{:stack, stack_name, next}, value}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)

        replicate(stack_name, :put, {{:stack, :counter, stack_name}, next})
        replicate(stack_name, :put, {{:stack, stack_name, next}, value})
        true
    end
  end

  ## Private — pop ─────────────────────────────────────────────────────────────

  defp stack_pop(partition, stack_name, default) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); stack_pop(partition, stack_name, default)
        end

      [{_, 0}] -> default

      [{_, counter}] ->
        next = counter - 1
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        value =
          case Storage.take({:stack, stack_name, counter}, partition) do
            [] ->
              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              replicate(stack_name, :put, {{:stack, :counter, stack_name}, 0})
              default

            [{_, v}] ->
              Storage.delete({:stack, stack_name, counter}, partition)
              Storage.put({{:stack, :counter, stack_name}, next}, partition)
              replicate(stack_name, :delete, {:stack, stack_name, counter})
              replicate(stack_name, :put, {{:stack, :counter, stack_name}, next})
              v
          end

        Storage.delete({:stack, :updating, stack_name}, partition)
        value
    end
  end

  ## Private — to_list (destructive drain) ─────────────────────────────────────

  defp to_list(partition, stack_name) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> []
          _  -> :erlang.yield(); to_list(partition, stack_name)
        end

      [{_, 0}] -> []

      [{_, counter}] ->
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        values =
          Enum.reduce(1..counter, [], fn x, acc ->
            case Storage.take({:stack, stack_name, x}, partition) do
              []       -> acc
              [{_, v}] ->
                replicate(stack_name, :delete, {:stack, stack_name, x})
                [v | acc]
            end
          end)

        Storage.put({{:stack, :counter, stack_name}, 0}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)
        replicate(stack_name, :put, {{:stack, :counter, stack_name}, 0})
        values
    end
  end

  ## Private — init / helpers ───────────────────────────────────────────────────

  defp stack_init(stack_name) do
    partition = Partition.get_partition(stack_name)
    Storage.put({{:stack, :counter, stack_name}, 0}, partition)
    replicate(stack_name, :put, {{:stack, :counter, stack_name}, 0})
  end

  defp replicate(stack_name, op, record_or_key) do
    idx = Partition.get_partition_order(stack_name)
    Replicator.replicate(idx, op, record_or_key)
  end

  defp primary_for(stack_name) do
    idx = Partition.get_partition_order(stack_name)
    {primary, _} = Manager.get_replicas(idx)
    primary
  end

  defp route(stack_name, fun, args) do
    primary = primary_for(stack_name)
    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn -> "super_cache, dist.stack #{inspect(stack_name)}, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
