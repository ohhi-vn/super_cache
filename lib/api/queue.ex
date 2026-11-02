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

  All structural mutations use a soft per-queue update lock with bounded
  spin-waiting and stale-lock reclamation so a crashed process never wedges
  the queue.

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

  alias SuperCache.Cluster.DistributedHelpers
  alias SuperCache.{Config, Partition, Storage}

  @max_count_retries 50
  @max_spin_retries 100
  @lock_stale_ms 2_000

  ## ── Public API ──────────────────────────────────────────────────────────────

  @doc "Enqueue `value` into `queue_name`. Creates the queue if it does not exist."
  @spec add(any, any) :: true
  def add(queue_name, value) do
    if Config.distributed?() do
      DistributedHelpers.route_write(
        __MODULE__,
        :dist_enqueue,
        [queue_name, value],
        idx(queue_name)
      )
    else
      local_enqueue(Partition.get_partition(queue_name), queue_name, value)
    end
  end

  @doc "Dequeue and return the front value. Returns `default` (`nil`) when empty."
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    if Config.distributed?() do
      DistributedHelpers.route_write(
        __MODULE__,
        :dist_dequeue,
        [queue_name, default],
        idx(queue_name)
      )
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
    if Config.distributed?() do
      DistributedHelpers.read_or_error(fn ->
        DistributedHelpers.route_read(
          __MODULE__,
          :dist_peek,
          [queue_name, default],
          idx(queue_name),
          opts
        )
      end)
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
    if Config.distributed?() do
      DistributedHelpers.read_or_error(fn ->
        DistributedHelpers.route_read(
          __MODULE__,
          :dist_count,
          [queue_name],
          idx(queue_name),
          opts
        )
      end)
    else
      count_safe(Partition.get_partition(queue_name), queue_name, @max_count_retries)
    end
  end

  @doc "Drain all items (oldest first). Returns `[]` for an empty queue."
  @spec get_all(any) :: list
  def get_all(queue_name) do
    if Config.distributed?() do
      DistributedHelpers.route_write(__MODULE__, :dist_drain, [queue_name], idx(queue_name))
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

  ## ── Private — local mode ────────────────────────────────────────────────────
  defp local_enqueue(partition, queue_name, value) do
    local_enqueue(partition, queue_name, value, 0)
  end

  defp local_enqueue(_partition, queue_name, _value, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, local_enqueue spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    false
  end

  defp local_enqueue(partition, queue_name, value, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          # An absent tail means the queue is uninitialized; treat it as 0.
          counter =
            case Storage.get({:queue, :tail, queue_name}, partition) do
              [{_, c}] -> c
              [] -> 0
            end

          if counter == 0 do
            Storage.delete({:queue, :head, queue_name}, partition)
            Storage.put({{:queue, queue_name, 1}, value}, partition)
            Storage.put({{:queue, :head, queue_name}, 1}, partition)
            Storage.put({{:queue, :tail, queue_name}, 1}, partition)
          else
            next = counter + 1
            Storage.put({{:queue, queue_name, next}, value}, partition)
            Storage.put({{:queue, :tail, queue_name}, next}, partition)
          end

          true
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        local_enqueue(partition, queue_name, value, retries + 1)
    end
  end

  defp local_dequeue(partition, queue_name, default) do
    local_dequeue(partition, queue_name, default, 0)
  end

  defp local_dequeue(_partition, queue_name, default, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, local_dequeue spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    default
  end

  defp local_dequeue(partition, queue_name, default, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          case Storage.get({:queue, :head, queue_name}, partition) do
            [] -> default
            [{_, 0}] -> default
            [{_, counter}] -> dequeue_at(partition, queue_name, counter, default)
          end
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        local_dequeue(partition, queue_name, default, retries + 1)
    end
  end

  defp dequeue_at(partition, queue_name, counter, default) do
    case Storage.take({:queue, queue_name, counter}, partition) do
      [] ->
        reset_queue(partition, queue_name)
        default

      [{_, value}] ->
        advance_head(partition, queue_name, counter + 1)
        value
    end
  end

  defp advance_head(partition, queue_name, next) do
    case Storage.get({:queue, :tail, queue_name}, partition) do
      [{_, tail}] when next > tail ->
        reset_queue(partition, queue_name)

      _ ->
        Storage.delete({:queue, :head, queue_name}, partition)
        Storage.put({{:queue, :head, queue_name}, next}, partition)
    end
  end

  defp local_drain(partition, queue_name) do
    local_drain(partition, queue_name, 0)
  end

  defp local_drain(_partition, queue_name, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, local_drain spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    []
  end

  defp local_drain(partition, queue_name, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          case Storage.get({:queue, :head, queue_name}, partition) do
            [] ->
              []

            [{_, 0}] ->
              []

            [{_, first}] ->
              [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)

              values =
                Enum.reduce(first..last, [], fn i, acc ->
                  case Storage.take({:queue, queue_name, i}, partition) do
                    [] -> acc
                    [{_, v}] -> [v | acc]
                  end
                end)
                |> Enum.reverse()

              reset_queue(partition, queue_name)
              values
          end
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        local_drain(partition, queue_name, retries + 1)
    end
  end

  ## ── Private — distributed mode ──────────────────────────────────────────────
  defp dist_do_enqueue(partition, queue_name, value) do
    dist_do_enqueue(partition, queue_name, value, 0)
  end

  defp dist_do_enqueue(_partition, queue_name, _value, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, dist_do_enqueue spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    false
  end

  defp dist_do_enqueue(partition, queue_name, value, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          counter =
            case Storage.get({:queue, :tail, queue_name}, partition) do
              [{_, c}] -> c
              [] -> 0
            end

          result =
            if counter == 0 do
              ops = [
                {:delete, {:queue, :head, queue_name}},
                {:put, {{:queue, queue_name, 1}, value}},
                {:put, {{:queue, :head, queue_name}, 1}},
                {:put, {{:queue, :tail, queue_name}, 1}}
              ]

              DistributedHelpers.apply_write(idx(queue_name), partition, ops)
            else
              next = counter + 1

              ops = [
                {:put, {{:queue, queue_name, next}, value}},
                {:put, {{:queue, :tail, queue_name}, next}}
              ]

              DistributedHelpers.apply_write(idx(queue_name), partition, ops)
            end

          case result do
            :ok -> true
            {:error, _} = error -> error
          end
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_enqueue(partition, queue_name, value, retries + 1)
    end
  end

  defp dist_do_dequeue(partition, queue_name, default) do
    dist_do_dequeue(partition, queue_name, default, 0)
  end

  defp dist_do_dequeue(_partition, queue_name, default, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, dist_do_dequeue spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    default
  end

  defp dist_do_dequeue(partition, queue_name, default, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          case Storage.get({:queue, :head, queue_name}, partition) do
            [] -> default
            [{_, 0}] -> default
            [{_, counter}] -> dist_dequeue_at(partition, queue_name, counter, default)
          end
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_dequeue(partition, queue_name, default, retries + 1)
    end
  end

  defp dist_dequeue_at(partition, queue_name, counter, default) do
    {value, ops} =
      case Storage.get({:queue, queue_name, counter}, partition) do
        [] -> {default, reset_ops(queue_name)}
        [{_, v}] -> {v, dequeue_ops(partition, queue_name, counter)}
      end

    case DistributedHelpers.apply_write(idx(queue_name), partition, ops) do
      :ok -> value
      {:error, _} = error -> error
    end
  end

  defp dequeue_ops(partition, queue_name, counter) do
    next = counter + 1

    case Storage.get({:queue, :tail, queue_name}, partition) do
      [{_, tail}] when next > tail ->
        [{:delete, {:queue, queue_name, counter}} | reset_ops(queue_name)]

      _ ->
        [
          {:delete, {:queue, queue_name, counter}},
          {:put, {{:queue, :head, queue_name}, next}}
        ]
    end
  end

  defp reset_ops(queue_name) do
    [
      {:put, {{:queue, :head, queue_name}, 0}},
      {:put, {{:queue, :tail, queue_name}, 0}}
    ]
  end

  defp dist_do_drain(partition, queue_name) do
    dist_do_drain(partition, queue_name, 0)
  end

  defp dist_do_drain(_partition, queue_name, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, dist_do_drain spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    []
  end

  defp dist_do_drain(partition, queue_name, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          drain_locked(partition, queue_name)
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_drain(partition, queue_name, retries + 1)
    end
  end

  defp drain_locked(partition, queue_name) do
    case Storage.get({:queue, :head, queue_name}, partition) do
      [] -> []
      [{_, 0}] -> []
      [{_, first}] -> drain_from(partition, queue_name, first)
    end
  end

  defp drain_from(partition, queue_name, first) do
    [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)

    {values, del_ops} =
      Enum.reduce(first..last, {[], []}, fn i, {vs, ops} ->
        case Storage.get({:queue, queue_name, i}, partition) do
          [] -> {vs, ops}
          [{_, v}] -> {[v | vs], [{:delete, {:queue, queue_name, i}} | ops]}
        end
      end)

    reset_ops = [
      {:put, {{:queue, :head, queue_name}, 0}},
      {:put, {{:queue, :tail, queue_name}, 0}}
    ]

    case DistributedHelpers.apply_write(
           idx(queue_name),
           partition,
           del_ops ++ reset_ops
         ) do
      :ok -> Enum.reverse(values)
      {:error, _} = error -> error
    end
  end

  ## ── Private — shared helpers ─────────────────────────────────────────────────
  defp local_peek(partition, queue_name, default) do
    local_peek(partition, queue_name, default, 0)
  end

  defp local_peek(_partition, queue_name, default, retries)
       when retries >= @max_spin_retries do
    Logger.error(
      "super_cache, queue, local_peek spin-wait exceeded #{@max_spin_retries} retries for #{inspect(queue_name)}"
    )

    default
  end

  defp local_peek(partition, queue_name, default, retries) do
    case acquire_lock(partition, queue_name) do
      :ok ->
        try do
          case Storage.get({:queue, :head, queue_name}, partition) do
            [] ->
              default

            [{_, 0}] ->
              default

            [{_, counter}] ->
              case Storage.get({:queue, queue_name, counter}, partition) do
                [] -> default
                [{_, v}] -> v
              end
          end
        after
          release_lock(partition, queue_name)
        end

      :contended ->
        Process.sleep(1)
        local_peek(partition, queue_name, default, retries + 1)
    end
  end

  defp count_safe(_part, _name, 0), do: 0

  defp count_safe(part, name, retries) do
    case Storage.get({:queue, :updating, name}, part) do
      [_] ->
        Process.sleep(1)
        count_safe(part, name, retries - 1)

      [] ->
        head = Storage.get({:queue, :head, name}, part)
        tail = Storage.get({:queue, :tail, name}, part)

        case {head, tail} do
          {[], []} ->
            0

          {[{_, 0}], _} ->
            0

          {_, [{_, 0}]} ->
            0

          {[], _} ->
            Process.sleep(1)
            count_safe(part, name, retries - 1)

          {[{_, h}], [{_, t}]} ->
            max(0, t - h + 1)
        end
    end
  end

  defp acquire_lock(partition, queue_name) do
    now = System.monotonic_time(:millisecond)
    lock_key = {:queue, :updating, queue_name}
    lock_value = {self(), now}

    if Storage.insert_new({lock_key, lock_value}, partition) do
      :ok
    else
      reclaim_lock(partition, lock_key, lock_value, now)
    end
  end

  defp reclaim_lock(partition, lock_key, lock_value, now) do
    case Storage.get(lock_key, partition) do
      [{_, {holder_pid, locked_at}}] ->
        if stale_lock?(holder_pid, locked_at, now) do
          replace_lock(partition, lock_key, lock_value)
        else
          :contended
        end

      [_] ->
        :contended

      [] ->
        :contended
    end
  end

  # A lock we hold ourselves is never stale: a slow caller must not have its
  # own critical section reclaimed underneath it.
  defp stale_lock?(holder_pid, _locked_at, _now) when holder_pid == self(), do: false

  defp stale_lock?(holder_pid, locked_at, now) do
    not Process.alive?(holder_pid) or now - locked_at > @lock_stale_ms
  end

  defp replace_lock(partition, lock_key, lock_value) do
    Storage.delete(lock_key, partition)

    if Storage.insert_new({lock_key, lock_value}, partition) do
      :ok
    else
      :contended
    end
  end

  defp release_lock(partition, queue_name) do
    lock_key = {:queue, :updating, queue_name}

    case Storage.get(lock_key, partition) do
      [{_, {pid, _}}] when pid == self() ->
        Storage.delete(lock_key, partition)

      _ ->
        :ok
    end
  end

  defp reset_queue(partition, name) do
    Storage.put({{:queue, :head, name}, 0}, partition)
    Storage.put({{:queue, :tail, name}, 0}, partition)
  end

  defp idx(name), do: Partition.get_partition_order(name)
end
