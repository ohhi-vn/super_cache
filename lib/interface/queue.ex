
defmodule SuperCache.Queue do
  @moduledoc """
  A queue based on cache.
  Data in queue stored in cache.
  Can handle multiple queue with different name.
  A queue is a FIFO data structure.
  This is global data, any process can access to queue data.
  Need to start SuperCache.start!/1 before using this module.
  """

  alias SuperCache.Storage
  alias SuperCache.Partition

  require Logger

  ### Api ###

  @doc """
  Add value to queue with name is queue_name.
  Queue is a FIFO data structure. If queue_name is not existed, it will be created.
  """
  @spec add(any, any) :: true
  def add(queue_name, value) do
    part = Partition.get_partition(queue_name)

    queue_in(part, queue_name, value)
  end

  @doc """
  Pop value from queue with name is queue_name.
  If queue_name is not existed or no data, it will return default value.
  """
  @spec out(any, any) :: any
  def out(queue_name, default \\ nil) do
    part = Partition.get_partition(queue_name)
    queue_out(part, queue_name, default)
  end

  ### Internal functions ###

  defp queue_in(partition, queue_name, value) do
    case Storage.take({:queue, :tail, queue_name}, partition) do
      [] -> # stack is not initialized
        case Storage.get({{:queue, :updating, queue_name}, :_}, partition) do
          [] -> # stack is not initialized
            queue_init(queue_name)
            queue_in(partition, queue_name, value)
          _ -> # queue is updating
            Process.sleep(0) # wait for stack is ready
            queue_in(partition, queue_name, value)
        end

      [{_, 0}] ->
        Storage.put({{:queue, :updating, queue_name}, true}, partition)

        Storage.delete({:queue, :tail, queue_name}, partition)
        Storage.delete({:queue, :head, queue_name}, partition)
        Storage.put({{:queue, queue_name, 1}, value}, partition)
        Storage.put({{:queue, :head, queue_name}, 1}, partition)
        Storage.put({{:queue, :tail, queue_name}, 1}, partition)

        Storage.delete({:queue, :updating, queue_name}, partition)
        Logger.debug("super_cache, queue, push value: #{inspect value} to queue #{inspect queue_name}")
        true
      [{_, counter}] ->
        next_counter = counter + 1
        Storage.put({{:queue, :updating, queue_name}, true}, partition)

        Storage.delete({:queue, :tail, queue_name}, partition)
        Storage.put({{:queue, queue_name, next_counter}, value}, partition)
        Storage.put({{:queue, :tail, queue_name}, next_counter}, partition)

        Storage.delete({:queue, :updating, queue_name}, partition)
        Logger.debug("super_cache, queue, push value: #{inspect value} to queue #{inspect queue_name}")
        true
    end
  end


  defp queue_out(partition, queue_name, default) do
    case Storage.take({:queue, :head, queue_name}, partition) do
      [] -> # stack is not initialized
        case Storage.get({:queue, :updating, queue_name}, partition) do
          [] -> # stack is not initialized
            default
          _ -> # queue is updating
            Process.sleep(0) # wait for stack is ready
            queue_out(partition, queue_name, default)

        end
      [{_, 0}] -> # queue is empty

        default
      [{_, counter}] ->
        next_counter = counter + 1
        Storage.put({{:queue, :updating, queue_name}, true}, partition)

        value =
          case Storage.take({:queue, queue_name, counter}, partition) do
            [] ->
              # no more data in queue, reset queue
              Storage.delete({:queue, :tail, queue_name}, partition)
              Storage.delete({:queue, :head, queue_name}, partition)
              Storage.put({{:queue, :head, queue_name}, 0}, partition)
              Storage.put({{:queue, :tail, queue_name}, 0}, partition)
              default
            [{_, value}] ->
              Storage.delete({:queue, :head, queue_name}, partition)
              Storage.put({{:queue, :head, queue_name}, next_counter}, partition)
              value
          end

        Storage.delete({:queue, :updating, queue_name}, partition)
        Logger.debug("super_cache, queue, push value: #{inspect value} to queue #{inspect queue_name}")

        value
    end
  end

  defp queue_init(queue_name) do
    Logger.debug("super_cache, stack, init stack: #{inspect queue_name}")
    partition = Partition.get_partition(queue_name)
    Storage.put({{:queue, :head, queue_name}, 0}, partition)
    Storage.put({{:queue, :tail, queue_name}, 0}, partition)
  end

end
