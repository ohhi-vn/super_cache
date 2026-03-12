# =============================================================================
# lib/super_cache/queue.ex
#
# Unified FIFO queue — absorbs SuperCache.Distributed.Queue.
#
# Local mode:  Storage.take (atomic destructive read) for the counter/tail.
# Distributed: Storage.get (non-destructive) + ops list so that 3PC can
#              apply the full batch atomically (primary applies last).
# =============================================================================
defmodule SuperCache.Queue do
  @moduledoc """
  Named FIFO queues backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  In distributed mode, structural mutations (`add`, `out`, `get_all`) are
  routed to the partition's primary node.  Reads (`peak`, `count`) default
  to the local replica and accept `:read_mode` for stronger consistency.

  ## Example

      alias SuperCache.Queue

      Queue.add("jobs", :compress)
      Queue.add("jobs", :upload)
      Queue.count("jobs")          # => 2
      Queue.peak("jobs")           # => :compress
      Queue.out("jobs")            # => :compress
      Queue.get_all("jobs")        # => [:upload]
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

  @max_count_retries 50

  ## ── Public API ──────────────────────────────────────────────────────────────

  @doc "Enqueue `value` into `queue_name`. Creates the queue if it does not exist."
  @spec add(any, any) :: true
  def add(queue_name, value) do
    if distributed?() do
      route_write(queue_name, :dist_enqueue, [queue_name, value])
    else
      local_enqueue(Partition.get_partition(queue_name), queue_name, value)
    end
  end

  @doc "Dequeue and return the front value. Returns `default` (`nil`) when empty."
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    if distributed?() do
      route_write(queue_name, :dist_dequeue, [queue_name, default])
    else
      local_dequeue(Partition.get_partition(queue_name), queue_name, default)
    end
  end

  @doc """
  Peek at the front value without removing it.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec peak(any, any, keyword) :: any
  def peak(queue_name, default \\ nil, opts \\ []) do
    if distributed?() do
      route_read(queue_name, :dist_peek, [queue_name, default], opts)
    else
      local_peek(Partition.get_partition(queue_name), queue_name, default)
    end
  end

  @doc """
  Return the number of items.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.
  """
  @spec count(any, keyword) :: non_neg_integer
  def count(queue_name, opts \\ []) do
    if distributed?() do
      route_read(queue_name, :dist_count, [queue_name], opts)
    else
      count_safe(Partition.get_partition(queue_name), queue_name, @max_count_retries)
    end
  end

  @doc "Drain all items (oldest first). Returns `[]` for an empty queue."
  @spec get_all(any) :: list
  def get_all(queue_name) do
    if distributed?() do
      route_write(queue_name, :dist_drain, [queue_name])
    else
      local_drain(Partition.get_partition(queue_name), queue_name)
    end
  end

  ## ── Remote entry points — called via :erpc on primary (distributed mode) ────

  @doc false
  def dist_enqueue(queue_name, value) do
    dist_do_enqueue(Partition.get_partition(queue_name), queue_name, value)
  end

  @doc false
  def dist_dequeue(queue_name, default) do
    dist_do_dequeue(Partition.get_partition(queue_name), queue_name, default)
  end

  @doc false
  def dist_peek(queue_name, default) do
    local_peek(Partition.get_partition(queue_name), queue_name, default)
  end

  @doc false
  def dist_count(queue_name) do
    count_safe(Partition.get_partition(queue_name), queue_name, @max_count_retries)
  end

  @doc false
  def dist_drain(queue_name) do
    dist_do_drain(Partition.get_partition(queue_name), queue_name)
  end

  ## ── Private — local mode (Storage.take for atomicity) ───────────────────────

  defp local_enqueue(partition, queue_name, value) do
    case Storage.take({:queue, :tail, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> local_init(queue_name); local_enqueue(partition, queue_name, value)
          _  -> :erlang.yield();        local_enqueue(partition, queue_name, value)
        end

      [{_, 0}] ->
        lock(partition, queue_name)
        Storage.delete({:queue, :head, queue_name}, partition)
        Storage.put({{:queue, queue_name, 1}, value}, partition)
        Storage.put({{:queue, :head, queue_name}, 1}, partition)
        Storage.put({{:queue, :tail, queue_name}, 1}, partition)
        unlock(partition, queue_name)
        true

      [{_, counter}] ->
        next = counter + 1
        lock(partition, queue_name)
        Storage.put({{:queue, queue_name, next}, value}, partition)
        Storage.put({{:queue, :tail, queue_name}, next}, partition)
        unlock(partition, queue_name)
        true
    end
  end

  defp local_dequeue(partition, queue_name, default) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] -> :erlang.yield(); local_dequeue(partition, queue_name, default)
      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          []        -> default
          [{_, 0}]  -> default
          [{_, counter}] ->
            lock(partition, queue_name)
            value = case Storage.take({:queue, queue_name, counter}, partition) do
              [] ->
                reset_queue(partition, queue_name)
                default
              [{_, v}] ->
                next = counter + 1
                case Storage.get({:queue, :tail, queue_name}, partition) do
                  [{_, tail}] when next > tail ->
                    reset_queue(partition, queue_name)
                  _ ->
                    Storage.delete({:queue, :head, queue_name}, partition)
                    Storage.put({{:queue, :head, queue_name}, next}, partition)
                end
                v
            end
            unlock(partition, queue_name)
            value
        end
    end
  end

  defp local_drain(partition, queue_name) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] -> :erlang.yield(); local_drain(partition, queue_name)
      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          []        -> []
          [{_, 0}]  -> []
          [{_, first}] ->
            lock(partition, queue_name)
            [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)
            values = Enum.reduce(first..last, [], fn i, acc ->
              case Storage.take({:queue, queue_name, i}, partition) do
                []       -> acc
                [{_, v}] -> [v | acc]
              end
            end) |> Enum.reverse()
            reset_queue(partition, queue_name)
            unlock(partition, queue_name)
            values
        end
    end
  end

  defp local_init(queue_name) do
    part = Partition.get_partition(queue_name)
    if Storage.insert_new({{:queue, :updating, queue_name}, true}, part) do
      Storage.put({{:queue, :head, queue_name}, 0}, part)
      Storage.put({{:queue, :tail, queue_name}, 0}, part)
      Storage.delete({:queue, :updating, queue_name}, part)
    end
  end

  ## ── Private — distributed mode (Storage.get + ops list for 3PC) ─────────────

  defp dist_do_enqueue(partition, queue_name, value) do
    case Storage.get({:queue, :tail, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> dist_init(queue_name); dist_do_enqueue(partition, queue_name, value)
          _  -> :erlang.yield();       dist_do_enqueue(partition, queue_name, value)
        end

      [{_, 0}] ->
        lock(partition, queue_name)
        ops = [{:delete, {:queue, :head, queue_name}},
               {:put, {{:queue, queue_name, 1}, value}},
               {:put, {{:queue, :head, queue_name}, 1}},
               {:put, {{:queue, :tail, queue_name}, 1}}]
        apply_write(idx(queue_name), partition, ops)
        unlock(partition, queue_name)
        true

      [{_, counter}] ->
        next = counter + 1
        lock(partition, queue_name)
        ops = [{:put, {{:queue, queue_name, next}, value}},
               {:put, {{:queue, :tail, queue_name}, next}}]
        apply_write(idx(queue_name), partition, ops)
        unlock(partition, queue_name)
        true
    end
  end

  defp dist_do_dequeue(partition, queue_name, default) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] -> :erlang.yield(); dist_do_dequeue(partition, queue_name, default)
      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          []        -> default
          [{_, 0}]  -> default
          [{_, counter}] ->
            lock(partition, queue_name)
            {value, ops} = case Storage.get({:queue, queue_name, counter}, partition) do
              [] ->
                {default, [{:put, {{:queue, :head, queue_name}, 0}},
                           {:put, {{:queue, :tail, queue_name}, 0}}]}
              [{_, v}] ->
                next = counter + 1
                ops  = case Storage.get({:queue, :tail, queue_name}, partition) do
                  [{_, tail}] when next > tail ->
                    [{:delete, {:queue, queue_name, counter}},
                     {:put, {{:queue, :head, queue_name}, 0}},
                     {:put, {{:queue, :tail, queue_name}, 0}}]
                  _ ->
                    [{:delete, {:queue, queue_name, counter}},
                     {:put, {{:queue, :head, queue_name}, next}}]
                end
                {v, ops}
            end
            apply_write(idx(queue_name), partition, ops)
            unlock(partition, queue_name)
            value
        end
    end
  end

  defp dist_do_drain(partition, queue_name) do
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] -> :erlang.yield(); dist_do_drain(partition, queue_name)
      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          []        -> []
          [{_, 0}]  -> []
          [{_, first}] ->
            lock(partition, queue_name)
            [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)
            {values, del_ops} = Enum.reduce(first..last, {[], []}, fn i, {vs, ops} ->
              case Storage.get({:queue, queue_name, i}, partition) do
                []       -> {vs, ops}
                [{_, v}] -> {[v | vs], [{:delete, {:queue, queue_name, i}} | ops]}
              end
            end)
            reset_ops = [{:put, {{:queue, :head, queue_name}, 0}},
                         {:put, {{:queue, :tail, queue_name}, 0}}]
            apply_write(idx(queue_name), partition, del_ops ++ reset_ops)
            unlock(partition, queue_name)
            Enum.reverse(values)
        end
    end
  end

  defp dist_init(queue_name) do
    part = Partition.get_partition(queue_name)
    if Storage.insert_new({{:queue, :updating, queue_name}, true}, part) do
      apply_write(idx(queue_name), part,
        [{:put, {{:queue, :head, queue_name}, 0}},
         {:put, {{:queue, :tail, queue_name}, 0}}])
      Storage.delete({:queue, :updating, queue_name}, part)
    end
  end

  ## ── Private — shared helpers ─────────────────────────────────────────────────

  defp local_peek(partition, queue_name, default) do
    case Storage.get({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> default
          _  -> :erlang.yield(); local_peek(partition, queue_name, default)
        end
      [{_, 0}]       -> default
      [{_, counter}] ->
        case Storage.get({:queue, queue_name, counter}, partition) do
          []       -> default
          [{_, v}] -> v
        end
    end
  end

  defp count_safe(_part, _name, 0), do: 0
  defp count_safe(part, name, retries) do
    case Storage.get({:queue, :updating, name}, part) do
      [_] -> :erlang.yield(); count_safe(part, name, retries - 1)
      [] ->
        head = Storage.get({:queue, :head, name}, part)
        tail = Storage.get({:queue, :tail, name}, part)
        case {head, tail} do
          {[], []}              -> 0
          {[{_, 0}], _}         -> 0
          {_, [{_, 0}]}         -> 0
          {[], _}               -> :erlang.yield(); count_safe(part, name, retries - 1)
          {[{_, h}], [{_, t}]}  -> max(0, t - h + 1)
        end
    end
  end

  defp lock(partition, name),   do: Storage.put({{:queue, :updating, name}, true}, partition)
  defp unlock(partition, name), do: Storage.delete({:queue, :updating, name}, partition)
  defp reset_queue(partition, name) do
    Storage.put({{:queue, :head, name}, 0}, partition)
    Storage.put({{:queue, :tail, name}, 0}, partition)
  end

  defp distributed?(), do: Config.get_config(:cluster, :local) == :distributed
  defp idx(name),      do: Partition.get_partition_order(name)

  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok         -> :ok
          {:error, r} -> Logger.error("super_cache, queue, 3pc failed: #{inspect(r)}"); {:error, r}
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

  defp route_write(queue_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(queue_name))
    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      SuperCache.Log.debug(fn -> "super_cache, queue #{inspect(queue_name)}, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read(queue_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)
    eff  = if mode == :local and not has_partition?(queue_name), do: :primary, else: mode
    case eff do
      :local   -> apply(__MODULE__, fun, args)
      :primary -> read_primary(queue_name, fun, args)
      :quorum  -> read_quorum(queue_name, fun, args)
    end
  end

  defp has_partition?(name) do
    {p, rs} = Manager.get_replicas(idx(name))
    node() in [p | rs]
  end

  defp read_primary(queue_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(queue_name))
    if primary == node(), do: apply(__MODULE__, fun, args),
    else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
  end

  defp read_quorum(queue_name, fun, args) do
    {primary, replicas} = Manager.get_replicas(idx(queue_name))
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
