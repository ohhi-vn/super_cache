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

  All structural mutations use a soft per-stack update lock with bounded
  spin-waiting and stale-lock reclamation so a crashed process never wedges
  the stack.
  """

  alias SuperCache.Cluster.DistributedHelpers
  alias SuperCache.{Config, Partition, Storage}

  @max_spin_retries 100
  @lock_stale_ms 2_000

  ## ── Public API ──────────────────────────────────────────────────────────────

  @doc "Push `value` onto `stack_name`. Creates the stack if it does not exist."
  @spec push(any, any) :: true
  def push(stack_name, value) do
    if Config.distributed?() do
      DistributedHelpers.route_write(__MODULE__, :dist_push, [stack_name, value], idx(stack_name))
    else
      local_push(Partition.get_partition(stack_name), stack_name, value)
    end
  end

  @doc "Pop and return the top value. Returns `default` (`nil`) when empty."
  @spec pop(any, any) :: any
  def pop(stack_name, default \\ nil) do
    if Config.distributed?() do
      DistributedHelpers.route_write(
        __MODULE__,
        :dist_pop,
        [stack_name, default],
        idx(stack_name)
      )
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
    if Config.distributed?() do
      DistributedHelpers.read_or_error(fn ->
        DistributedHelpers.route_read(
          __MODULE__,
          :dist_count,
          [stack_name],
          idx(stack_name),
          opts
        )
      end)
    else
      local_count(stack_name)
    end
  end

  @doc "Drain all items top-first. Returns `[]` for an empty stack."
  @spec get_all(any) :: list
  def get_all(stack_name) do
    if Config.distributed?() do
      DistributedHelpers.route_write(__MODULE__, :dist_get_all, [stack_name], idx(stack_name))
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

  ## ── Private — local mode ────────────────────────────────────────────────────

  defp local_push(partition, stack_name, value) do
    local_push(partition, stack_name, value, 0)
  end

  defp local_push(_partition, _stack_name, _value, retries)
       when retries >= @max_spin_retries do
    false
  end

  defp local_push(partition, stack_name, value, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          case Storage.get({:stack, :counter, stack_name}, partition) do
            [] ->
              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              do_push(partition, stack_name, value, 0)

            [{_, counter}] ->
              do_push(partition, stack_name, value, counter)
          end
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        local_push(partition, stack_name, value, retries + 1)
    end
  end

  defp do_push(partition, stack_name, value, counter) do
    next = counter + 1
    Storage.put({{:stack, :counter, stack_name}, next}, partition)
    Storage.put({{:stack, stack_name, next}, value}, partition)
    true
  end

  defp local_pop(partition, stack_name, default) do
    local_pop(partition, stack_name, default, 0)
  end

  defp local_pop(_partition, _stack_name, default, retries)
       when retries >= @max_spin_retries do
    default
  end

  defp local_pop(partition, stack_name, default, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          case Storage.get({:stack, :counter, stack_name}, partition) do
            [] ->
              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              default

            [{_, 0}] ->
              default

            [{_, counter}] ->
              case Storage.take({:stack, stack_name, counter}, partition) do
                [] ->
                  Storage.put({{:stack, :counter, stack_name}, 0}, partition)
                  default

                [{_, v}] ->
                  Storage.put({{:stack, :counter, stack_name}, counter - 1}, partition)
                  v
              end
          end
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        local_pop(partition, stack_name, default, retries + 1)
    end
  end

  defp local_drain(partition, stack_name) do
    local_drain(partition, stack_name, 0)
  end

  defp local_drain(_partition, _stack_name, retries)
       when retries >= @max_spin_retries do
    []
  end

  defp local_drain(partition, stack_name, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          case Storage.get({:stack, :counter, stack_name}, partition) do
            [] ->
              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              []

            [{_, 0}] ->
              []

            [{_, counter}] ->
              values =
                Enum.reduce(counter..1//-1, [], fn x, acc ->
                  case Storage.take({:stack, stack_name, x}, partition) do
                    [] -> acc
                    [{_, v}] -> [v | acc]
                  end
                end)

              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              values
          end
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        local_drain(partition, stack_name, retries + 1)
    end
  end

  defp local_count(stack_name) do
    part = Partition.get_partition(stack_name)

    case Storage.get({:stack, :counter, stack_name}, part) do
      [] -> 0
      [{_, counter}] -> counter
    end
  end

  ## ── Private — distributed mode ──────────────────────────────────────────────

  defp dist_do_push(partition, stack_name, value) do
    dist_do_push(partition, stack_name, value, 0)
  end

  defp dist_do_push(_partition, _stack_name, _value, retries)
       when retries >= @max_spin_retries do
    false
  end

  defp dist_do_push(partition, stack_name, value, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          result =
            case Storage.get({:stack, :counter, stack_name}, partition) do
              [] ->
                DistributedHelpers.apply_write(idx(stack_name), partition, [
                  {:put, {{:stack, :counter, stack_name}, 1}},
                  {:put, {{:stack, stack_name, 1}, value}}
                ])

              [{_, counter}] ->
                next = counter + 1

                DistributedHelpers.apply_write(idx(stack_name), partition, [
                  {:put, {{:stack, :counter, stack_name}, next}},
                  {:put, {{:stack, stack_name, next}, value}}
                ])
            end

          case result do
            :ok -> true
            {:error, _} = error -> error
          end
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_push(partition, stack_name, value, retries + 1)
    end
  end

  defp dist_do_pop(partition, stack_name, default) do
    dist_do_pop(partition, stack_name, default, 0)
  end

  defp dist_do_pop(_partition, _stack_name, default, retries)
       when retries >= @max_spin_retries do
    default
  end

  defp dist_do_pop(partition, stack_name, default, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          pop_locked(partition, stack_name, default)
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_pop(partition, stack_name, default, retries + 1)
    end
  end

  defp pop_locked(partition, stack_name, default) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] -> default
      [{_, 0}] -> default
      [{_, counter}] -> pop_at(partition, stack_name, counter, default)
    end
  end

  defp pop_at(partition, stack_name, counter, default) do
    case Storage.get({:stack, stack_name, counter}, partition) do
      [] ->
        apply_value(
          [{:put, {{:stack, :counter, stack_name}, 0}}],
          default,
          partition,
          stack_name
        )

      [{_, v}] ->
        apply_value(
          [
            {:delete, {:stack, stack_name, counter}},
            {:put, {{:stack, :counter, stack_name}, counter - 1}}
          ],
          v,
          partition,
          stack_name
        )
    end
  end

  defp dist_do_drain(partition, stack_name) do
    dist_do_drain(partition, stack_name, 0)
  end

  defp dist_do_drain(_partition, _stack_name, retries)
       when retries >= @max_spin_retries do
    []
  end

  defp dist_do_drain(partition, stack_name, retries) do
    case acquire_lock(partition, stack_name) do
      :ok ->
        try do
          drain_locked(partition, stack_name)
        after
          release_lock(partition, stack_name)
        end

      :contended ->
        Process.sleep(1)
        dist_do_drain(partition, stack_name, retries + 1)
    end
  end

  defp drain_locked(partition, stack_name) do
    case Storage.get({:stack, :counter, stack_name}, partition) do
      [] -> []
      [{_, 0}] -> []
      [{_, counter}] -> drain_from(partition, stack_name, counter)
    end
  end

  defp drain_from(partition, stack_name, counter) do
    {values, del_ops} =
      Enum.reduce(1..counter, {[], []}, fn x, {vs, ops} ->
        case Storage.get({:stack, stack_name, x}, partition) do
          [] -> {vs, ops}
          [{_, v}] -> {[v | vs], [{:delete, {:stack, stack_name, x}} | ops]}
        end
      end)

    reset_ops = [{:put, {{:stack, :counter, stack_name}, 0}}]
    apply_value(del_ops ++ reset_ops, values, partition, stack_name)
  end

  defp apply_value(ops, value, partition, stack_name) do
    case DistributedHelpers.apply_write(idx(stack_name), partition, ops) do
      :ok -> value
      {:error, _} = error -> error
    end
  end

  ## ── Private — shared helpers ─────────────────────────────────────────────────

  defp acquire_lock(partition, stack_name) do
    now = System.monotonic_time(:millisecond)
    lock_key = {:stack, :updating, stack_name}
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

      [_legacy_or_malformed] ->
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

  defp release_lock(partition, stack_name) do
    lock_key = {:stack, :updating, stack_name}

    case Storage.get(lock_key, partition) do
      [{_, {pid, _}}] when pid == self() ->
        Storage.delete(lock_key, partition)

      _ ->
        :ok
    end
  end

  defp idx(name), do: Partition.get_partition_order(name)
end
