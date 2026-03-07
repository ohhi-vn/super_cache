defmodule SuperCache.Queue do
  @moduledoc """
  Named FIFO queues backed by SuperCache ETS partitions.

  Any number of independent queues can coexist by using different
  `queue_name` values.  Any process in the VM can enqueue and dequeue from
  the same queue by name.

  Requires `SuperCache.start!/1` to be called first.

  ## Concurrency model

  Queue mutations (enqueue, dequeue, drain) use a soft write-lock stored in
  ETS.  The lock is a single `{:queue, :updating, queue_name}` record that
  is inserted before a structural mutation and deleted immediately after.
  Concurrent callers that observe the lock entry spin via `:erlang.yield/0`
  and retry.  This keeps the implementation lock-free at the OTP level while
  still serialising mutations to each queue.

  ## Storage layout

  Each queue is represented by three kinds of records in the same partition:

  | ETS key                           | Value    | Purpose              |
  |-----------------------------------|----------|----------------------|
  | `{:queue, :head, queue_name}`     | integer  | Index of front item  |
  | `{:queue, :tail, queue_name}`     | integer  | Index of back item   |
  | `{:queue, queue_name, index}`     | any      | The item itself      |

  Head and tail are both `0` when the queue is empty.

  ## Example

      alias SuperCache.Queue

      SuperCache.start!()

      Queue.add("jobs", :compress)
      Queue.add("jobs", :upload)
      Queue.add("jobs", :notify)

      Queue.count("jobs")       # => 3
      Queue.peak("jobs")        # => :compress  (non-destructive)

      Queue.out("jobs")         # => :compress
      Queue.out("jobs")         # => :upload

      Queue.get_all("jobs")     # => [:notify]  (drains the queue)
      Queue.count("jobs")       # => 0

      Queue.out("jobs")         # => nil
      Queue.out("jobs", :empty) # => :empty

  ## Cluster mode

  For distributed deployments use `SuperCache.Distributed.Queue`.  The API
  is identical; mutations are routed to the primary node for the queue's
  partition.
  """

  alias SuperCache.{Storage, Partition}
  require Logger

  ## API ────────────────────────────────────────────────────────────────────────

  @doc """
  Enqueue `value` into `queue_name`.

  Creates the queue if it does not yet exist.  Returns `true`.

  ## Example

      Queue.add("tasks", %{id: 1, type: :email})
  """
  @spec add(any, any) :: true
  def add(queue_name, value) do
    part = Partition.get_partition(queue_name)
    queue_in(part, queue_name, value)
  end

  @doc """
  Dequeue and return the front value.

  Returns `default` (default `nil`) when the queue is empty or does not
  exist.

  ## Example

      Queue.add("q", :first)
      Queue.add("q", :second)
      Queue.out("q")            # => :first
      Queue.out("q")            # => :second
      Queue.out("q")            # => nil
      Queue.out("q", :empty)    # => :empty
  """
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_out(part, queue_name, default)
  end

  @doc """
  Return the front value **without removing it**.

  Returns `default` (default `nil`) when the queue is empty or does not
  exist.

  ## Example

      Queue.add("q", :hello)
      Queue.peak("q")        # => :hello
      Queue.count("q")       # => 1  (unchanged)
  """
  @spec peak(any, any) :: any
  def peak(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_peak(part, queue_name, default)
  end

  @doc """
  Return the number of items in `queue_name`.

  Returns `0` for an empty or non-existent queue.

  ## Example

      Queue.add("q", :a)
      Queue.add("q", :b)
      Queue.count("q")   # => 2
      Queue.out("q")
      Queue.count("q")   # => 1
  """
  @spec count(any) :: non_neg_integer
  def count(queue_name) do
    part = Partition.get_partition(queue_name)
    count_safe(part, queue_name, 50)
  end

  @doc """
  Remove and return **all** items as a list (oldest first).

  The queue is empty after this call.  Returns `[]` for an empty or
  non-existent queue.

  ## Example

      Enum.each(1..5, &Queue.add("q", &1))
      Queue.get_all("q")     # => [1, 2, 3, 4, 5]
      Queue.count("q")       # => 0
  """
  @spec get_all(any) :: list
  def get_all(queue_name) do
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
        Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, in: #{inspect(value)}" end)
        true

      [{_, counter}] ->
        next = counter + 1
        lock(partition, queue_name)
        Storage.put({{:queue, queue_name, next}, value}, partition)
        Storage.put({{:queue, :tail, queue_name}, next}, partition)
        unlock(partition, queue_name)
        Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, in: #{inspect(value)}" end)
        true
    end
  end

  ## Private — queue_out ───────────────────────────────────────────────────────

  defp queue_out(partition, queue_name, default) do
    # If no lock record exists yet the queue may not be initialised.
    case Storage.get({:queue, :updating, queue_name}, partition) do
      [_] ->
        # Another process is mutating — yield and retry.
        :erlang.yield()
        queue_out(partition, queue_name, default)

      [] ->
        case Storage.get({:queue, :head, queue_name}, partition) do
          [] ->
            # Queue does not exist.
            default

          [{_, 0}] ->
            # Queue is empty.
            default

          [{_, counter}] ->
            # Acquire lock, then perform the dequeue entirely inside it.
            lock(partition, queue_name)

            value =
              case Storage.take({:queue, queue_name, counter}, partition) do
                [] ->
                  # Item missing (shouldn't happen under correct usage).
                  reset_queue(partition, queue_name)
                  default

                [{_, v}] ->
                  next = counter + 1

                  case Storage.get({:queue, :tail, queue_name}, partition) do
                    [{_, tail}] when next > tail ->
                      reset_queue(partition, queue_name)
                    _ ->
                      # Delete the old head key, write the new one.
                      Storage.delete({:queue, :head, queue_name}, partition)
                      Storage.put({{:queue, :head, queue_name}, next}, partition)
                  end

                  v
              end

            unlock(partition, queue_name)
            Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, out: #{inspect(value)}" end)
            value
        end
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

      [{_, 0}]       -> default
      [{_, counter}] ->
        case Storage.get({:queue, queue_name, counter}, partition) do
          []       -> default
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
          []       -> []
          [{_, 0}] -> []

          [{_, first}] ->
            lock(partition, queue_name)
            [{_, last}] = Storage.get({:queue, :tail, queue_name}, partition)

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
            Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, drained #{length(values)} item(s)" end)
            values
        end
    end
  end

  ## Private — init / lock helpers ─────────────────────────────────────────────

  defp queue_init(queue_name) do
    Logger.debug("super_cache, queue, init: #{inspect(queue_name)}")
    partition = Partition.get_partition(queue_name)

    # insert_new acts as a CAS — only the first caller proceeds.
    if Storage.insert_new({{:queue, :updating, queue_name}, true}, partition) do
      Storage.put({{:queue, :head, queue_name}, 0}, partition)
      Storage.put({{:queue, :tail, queue_name}, 0}, partition)
      Storage.delete({:queue, :updating, queue_name}, partition)
    end
  end

  defp lock(partition, queue_name),
    do: Storage.put({{:queue, :updating, queue_name}, true}, partition)

  defp unlock(partition, queue_name),
    do: Storage.delete({:queue, :updating, queue_name}, partition)

  defp reset_queue(partition, queue_name) do
    Storage.put({{:queue, :head, queue_name}, 0}, partition)
    Storage.put({{:queue, :tail, queue_name}, 0}, partition)
  end

  defp count_safe(_part, _queue_name, 0),
    do: 0   # give up after max retries rather than looping forever

  defp count_safe(part, queue_name, retries) do
    # Refuse to read while a structural mutation is in progress.
    case Storage.get({:queue, :updating, queue_name}, part) do
      [_] ->
        :erlang.yield()
        count_safe(part, queue_name, retries - 1)

      [] ->
        tail = Storage.get({:queue, :tail, queue_name}, part)
        head = Storage.get({:queue, :head, queue_name}, part)

        case {head, tail} do
          # Queue does not exist yet.
          {[], []}               -> 0
          # Empty queue sentinel.
          {[{_, 0}], _}          -> 0
          {_, [{_, 0}]}          -> 0
          # Torn read — a mutation landed between our two gets; retry.
          {[], _} ->
            :erlang.yield()
            count_safe(part, queue_name, retries - 1)
          # Clean read.
          {[{_, h}], [{_, t}]}   -> max(0, t - h + 1)
        end
    end
  end

end
