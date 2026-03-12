

# =============================================================================
# lib/super_cache/stack.ex
#
# Unified LIFO stack — absorbs SuperCache.Distributed.Stack.
# Same dual-implementation pattern as Queue.
# =============================================================================
defmodule SuperCache.Stack do
  @moduledoc """
  Named LIFO stacks backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  In distributed mode, structural mutations (`push`, `pop`, `get_all`) are
  routed to the partition's primary node.

  ## Example

      alias SuperCache.Stack

      Stack.push("history", :page_a)
      Stack.push("history", :page_b)
      Stack.pop("history")          # => :page_b
      Stack.get_all("history")      # => [:page_a]
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

  ## ── Public API ──────────────────────────────────────────────────────────────

  @doc "Push `value` onto `stack_name`. Creates the stack if it does not exist."
  @spec push(any, any) :: true
  def push(stack_name, value) do
    if distributed?() do
      route_write(stack_name, :dist_push, [stack_name, value])
    else
      local_push(Partition.get_partition(stack_name), stack_name, value)
    end
  end

  @doc "Pop and return the top value. Returns `default` (`nil`) when empty."
  @spec pop(any, any) :: any
  def pop(stack_name, default \\ nil) do
    if distributed?() do
      route_write(stack_name, :dist_pop, [stack_name, default])
    else
      local_pop(Partition.get_partition(stack_name), stack_name, default)
    end
  end

  @doc """
  Return the number of items.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec count(any, keyword) :: non_neg_integer
  def count(stack_name, opts \\ []) do
    if distributed?() do
      route_read(stack_name, :dist_count, [stack_name], opts)
    else
      local_count(stack_name)
    end
  end

  @doc "Drain all items top-first. Returns `[]` for an empty stack."
  @spec get_all(any) :: list
  def get_all(stack_name) do
    if distributed?() do
      route_write(stack_name, :dist_get_all, [stack_name])
    else
      local_drain(Partition.get_partition(stack_name), stack_name)
    end
  end

  ## ── Remote entry points (distributed primary, called via :erpc) ─────────────

  @doc false
  def dist_push(stack_name, value) do
    dist_do_push(Partition.get_partition(stack_name), stack_name, value)
  end

  @doc false
  def dist_pop(stack_name, default) do
    dist_do_pop(Partition.get_partition(stack_name), stack_name, default)
  end

  @doc false
  def dist_count(stack_name), do: local_count(stack_name)

  @doc false
  def dist_get_all(stack_name) do
    dist_do_drain(Partition.get_partition(stack_name), stack_name)
  end

  ## ── Private — local mode (Storage.take for atomicity) ───────────────────────

  defp local_push(partition, stack_name, value) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> local_init(stack_name); local_push(partition, stack_name, value)
          _  -> :erlang.yield();        local_push(partition, stack_name, value)
        end
      [{_, counter}] ->
        next = counter + 1
        Storage.put({{:stack, :updating, stack_name}, true}, partition)
        Storage.put({{:stack, :counter, stack_name}, next}, partition)
        Storage.put({{:stack, stack_name, next}, value}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)
        true
    end
  end

  defp local_pop(partition, stack_name, default) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); local_pop(partition, stack_name, default)
        end
      [{_, 0}] -> default
      [{_, counter}] ->
        Storage.put({{:stack, :updating, stack_name}, true}, partition)
        value = case Storage.take({:stack, stack_name, counter}, partition) do
          []       -> Storage.put({{:stack, :counter, stack_name}, 0}, partition); default
          [{_, v}] -> Storage.put({{:stack, :counter, stack_name}, counter - 1}, partition); v
        end
        Storage.delete({:stack, :updating, stack_name}, partition)
        value
    end
  end

  defp local_drain(partition, stack_name) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> []
          _  -> :erlang.yield(); local_drain(partition, stack_name)
        end
      [{_, 0}] -> []
      [{_, counter}] ->
        Storage.put({{:stack, :updating, stack_name}, true}, partition)
        values = Enum.reduce(counter..1//-1, [], fn x, acc ->
          case Storage.take({:stack, stack_name, x}, partition) do
            []       -> acc
            [{_, v}] -> [v | acc]
          end
        end)
        Storage.put({{:stack, :counter, stack_name}, 0}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)
        values
    end
  end

  defp local_init(stack_name) do
    part = Partition.get_partition(stack_name)
    Storage.put({{:stack, :counter, stack_name}, 0}, part)
  end

  defp local_count(stack_name) do
    part = Partition.get_partition(stack_name)
    case Storage.get({:stack, :counter, stack_name}, part) do
      []             -> 0
      [{_, counter}] -> counter
    end
  end

  ## ── Private — distributed mode (Storage.get + ops list for 3PC) ─────────────

  defp dist_do_push(partition, stack_name, value) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> dist_init(stack_name); dist_do_push(partition, stack_name, value)
          _  -> :erlang.yield();       dist_do_push(partition, stack_name, value)
        end
      [{_, counter}] ->
        next = counter + 1
        lock(partition, stack_name)
        ops = [{:put, {{:stack, :counter, stack_name}, next}},
               {:put, {{:stack, stack_name, next}, value}}]
        apply_write(idx(stack_name), partition, ops)
        unlock(partition, stack_name)
        true
    end
  end

  defp dist_do_pop(partition, stack_name, default) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({:stack, :updating, stack_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); dist_do_pop(partition, stack_name, default)
        end
      [{_, 0}] -> default
      [{_, counter}] ->
        lock(partition, stack_name)
        {value, ops} = case Storage.get({:stack, stack_name, counter}, partition) do
          [] ->
            {default, [{:put, {{:stack, :counter, stack_name}, 0}}]}
          [{_, v}] ->
            {v, [{:delete, {:stack, stack_name, counter}},
                 {:put, {{:stack, :counter, stack_name}, counter - 1}}]}
        end
        apply_write(idx(stack_name), partition, ops)
        unlock(partition, stack_name)
        value
    end
  end

  defp dist_do_drain(partition, stack_name) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      []        -> []
      [{_, 0}]  -> []
      [{_, counter}] ->
        lock(partition, stack_name)
        {values, del_ops} = Enum.reduce(1..counter, {[], []}, fn x, {vs, ops} ->
          case Storage.get({:stack, stack_name, x}, partition) do
            []       -> {vs, ops}
            [{_, v}] -> {[v | vs], [{:delete, {:stack, stack_name, x}} | ops]}
          end
        end)
        reset_ops = [{:put, {{:stack, :counter, stack_name}, 0}}]
        apply_write(idx(stack_name), partition, del_ops ++ reset_ops)
        unlock(partition, stack_name)
        values
    end
  end

  defp dist_init(stack_name) do
    part = Partition.get_partition(stack_name)
    apply_write(idx(stack_name), part, [{:put, {{:stack, :counter, stack_name}, 0}}])
  end

  ## ── Private — shared helpers ─────────────────────────────────────────────────

  defp lock(partition, name),   do: Storage.put({{:stack, :updating, name}, true}, partition)
  defp unlock(partition, name), do: Storage.delete({:stack, :updating, name}, partition)

  defp distributed?(), do: Config.get_config(:cluster, :local) == :distributed
  defp idx(name),      do: Partition.get_partition_order(name)

  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok         -> :ok
          {:error, r} -> Logger.error("super_cache, stack, 3pc failed: #{inspect(r)}"); {:error, r}
        end
      _ ->
        Enum.each(ops, fn
          {:put, r}          -> Storage.put(r, partition);          Replicator.replicate(idx, :put, r)
          {:delete, k}       -> Storage.delete(k, partition);       Replicator.replicate(idx, :delete, k)
          {:delete_match, p} -> Storage.delete_match(p, partition); Replicator.replicate(idx, :delete_match, p)
          {:delete_all, _}   -> Storage.delete_all(partition);      Replicator.replicate(idx, :delete_all, nil)
        end)
        :ok
    end
  end

  defp route_write(stack_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(stack_name))
    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      SuperCache.Log.debug(fn -> "super_cache, stack #{inspect(stack_name)}, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read(stack_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)
    eff  = if mode == :local and not has_partition?(stack_name), do: :primary, else: mode
    case eff do
      :local   -> apply(__MODULE__, fun, args)
      :primary ->
        {primary, _} = Manager.get_replicas(idx(stack_name))
        if primary == node(), do: apply(__MODULE__, fun, args),
        else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
      :quorum ->
        {primary, replicas} = Manager.get_replicas(idx(stack_name))
        results = [primary | replicas]
        |> Task.async_stream(
          fn n when n == node() -> apply(__MODULE__, fun, args)
             n                  -> :erpc.call(n, __MODULE__, fun, args, 5_000)
          end,
          timeout: 5_000, on_timeout: :kill_task
        )
        |> Enum.flat_map(fn {:ok, r} -> [r]; _ -> [] end)
        majority = div(length(results), 2) + 1
        case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
          {result, _} -> result
          nil -> if primary == node(), do: apply(__MODULE__, fun, args),
                 else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
        end
    end
  end

  defp has_partition?(name) do
    {p, rs} = Manager.get_replicas(idx(name))
    node() in [p | rs]
  end
end
