defmodule SuperCache.Queue do
  @moduledoc """
  A named, FIFO queue backed by SuperCache ETS storage.

  Any process in the VM can read or write any queue by name.
  Multiple queues with different names can coexist.

  Requires `SuperCache.start!/1` to be called first.

  ## Example

      alias SuperCache.Queue

      SuperCache.start!()
      Queue.add("jobs", :task_a)
      Queue.add("jobs", :task_b)
      Queue.out("jobs")    # => :task_a
      Queue.count("jobs")  # => 1
      Queue.get_all("jobs") # => [:task_b]  (non-destructive)
  """

  alias SuperCache.{Storage, Partition}

  require Logger

  ## API ##

  @doc """
  Enqueue `value` into the named queue.
  Creates the queue if it does not yet exist.
  """
  @spec add(any, any) :: true
  def add(queue_name, value) do
    part = Partition.get_partition(queue_name)
    queue_in(part, queue_name, value)
  end

  @doc """
  Dequeue and return the front value.
  Returns `default` (default `nil`) when the queue is empty or does not exist.
  """
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_out(part, queue_name, default)
  end

  @doc """
  Return the front value without removing it.
  Returns `default` (default `nil`) when the queue is empty or does not exist.
  """
  @spec peak(any, any) :: any
  def peak(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_peak(part, queue_name, default)
  end

  @doc """
  Return the number of items in the queue.
  Returns `0` when the queue is empty or does not exist.
  """
  @spec count(any) :: non_neg_integer
  def count(queue_name) do
    part = Partition.get_partition(queue_name)

    case Storage.get({:queue, :tail, queue_name}, part) do
      [] ->
        0

      [{_, tail}] ->
        case Storage.get({:queue, :head, queue_name}, part) do
          # Caught mid-update — retry.
          [] -> count(queue_name)
          [{_, 0}] -> 0
          [{_, head}] -> tail - head + 1
        end
    end
  end

  @doc """
  Remove and return all items in the queue as a list (oldest first).

  **This operation drains the queue.** After the call the queue is empty.
  """
  @spec get_all(any) :: list
  def get_all(queue_name) do
    part = Partition.get_partition(queue_name)
    queue_drain(part, queue_name)
  end

  ## Private — queue_in ##

  defp queue_in(partition, queue_name, value) do
    case Storage.take({:queue, :tail, queue_name}, partition) do
      # Queue does not exist — initialise then retry.
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] ->
            # Truly uninitialised — we own the init.
            queue_init(queue_name)
            queue_in(partition, queue_name, value)

          _ ->
            # Another process is initialising — wait and retry.
            :erlang.yield()
            queue_in(partition, queue_name, value)
        end

      # Queue exists but is empty — write first element.
      [{_, 0}] ->
        lock(partition, queue_name)
        Storage.delete({:queue, :head, queue_name}, partition)
        Storage.put({{:queue, queue_name, 1}, value}, partition)
        Storage.put({{:queue, :head, queue_name}, 1}, partition)
        Storage.put({{:queue, :tail, queue_name}, 1}, partition)
        unlock(partition, queue_name)

        Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, in: #{inspect(value)}" end)
        true

      # Queue has existing items — append.
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

  ## Private — queue_out ##

  defp queue_out(partition, queue_name, default) do
    case Storage.take({:queue, :head, queue_name}, partition) do
      # Queue does not exist.
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] ->
            default

          _ ->
            :erlang.yield()
            queue_out(partition, queue_name, default)
        end

      # Queue is empty.
      [{_, 0}] ->
        default

      [{_, counter}] ->
        lock(partition, queue_name)

        value =
          case Storage.take({:queue, queue_name, counter}, partition) do
            [] ->
              # Ran off the end — reset and return default.
              reset_queue(partition, queue_name)
              default

            [{_, v}] ->
              next = counter + 1

              case Storage.get({:queue, :tail, queue_name}, partition) do
                [{_, tail}] when next > tail ->
                  # Just dequeued the last item — reset to empty.
                  reset_queue(partition, queue_name)

                _ ->
                  Storage.put({{:queue, :head, queue_name}, next}, partition)
              end

              v
          end

        unlock(partition, queue_name)
        Logger.debug(fn -> "super_cache, queue #{inspect(queue_name)}, out: #{inspect(value)}" end)
        value
    end
  end

  ## Private — queue_peak ##

  defp queue_peak(partition, queue_name, default) do
    case Storage.get({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> default
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

  ## Private — drain (destructive read) ##

  defp queue_drain(partition, queue_name) do
    case Storage.take({:queue, :head, queue_name}, partition) do
      [] ->
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] ->
            []

          _ ->
            :erlang.yield()
            queue_drain(partition, queue_name)
        end

      [{_, 0}] ->
        []

      [{_, first}] ->
        lock(partition, queue_name)
        [{_, last}] = Storage.take({:queue, :tail, queue_name}, partition)

        values =
          Enum.reduce(first..last, [], fn i, acc ->
            case Storage.take({:queue, queue_name, i}, partition) do
              [] -> acc
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

  ## Private — init / lock helpers ##

  defp queue_init(queue_name) do
    Logger.debug("super_cache, queue, init: #{inspect(queue_name)}")
    partition = Partition.get_partition(queue_name)

    # insert_new acts as CAS — only the first caller proceeds.
    if Storage.insert_new({{:queue, :updating, queue_name}, true}, partition) do
      Storage.put({{:queue, :head, queue_name}, 0}, partition)
      Storage.put({{:queue, :tail, queue_name}, 0}, partition)
      Storage.delete({:queue, :updating, queue_name}, partition)
    end
  end

  # Soft write lock — set before structural mutations, cleared after.
  defp lock(partition, queue_name) do
    Storage.put({{:queue, :updating, queue_name}, true}, partition)
  end

  defp unlock(partition, queue_name) do
    Storage.delete({:queue, :updating, queue_name}, partition)
  end

  defp reset_queue(partition, queue_name) do
    Storage.put({{:queue, :head, queue_name}, 0}, partition)
    Storage.put({{:queue, :tail, queue_name}, 0}, partition)
  end
end
