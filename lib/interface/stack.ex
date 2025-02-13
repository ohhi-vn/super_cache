
defmodule SuperCache.Stack do
  @moduledoc """
  Stack module helps to easy to use stack data structure.
  This is global stack, any process can access to stack data.
  Can handle multiple stack with different name.
  Need to start SuperCache.start!/1 before using this module.

  Ex:
  ```
  alias SuperCache.Stack
  SuperCache.start!()
  Stack.push("my_stack", "Hello")
  Stack.pop("my_stack")
    # => "Hello"
  ```
  """

  alias SuperCache.Storage
  alias SuperCache.Partition

  require Logger

  ### Api ###


  @doc """
  Add value to stack has name is stack_name.
  If stack_name is not existed, it will be created.
  """
  @spec push(any, any) :: true
  def push(stack_name, value) do
    part = Partition.get_partition(stack_name)

    stack_push(part, stack_name, value)
  end

  @doc """
  Pop value from stack with name is stack_name.
  If stack_name is not existed or no data, it will return default value.
  """
  @spec pop(any, any) :: any
  def pop(stack_name, default \\nil) do
    part = Partition.get_partition(stack_name)
    stack_pop(part, stack_name, default)
  end

  def count(stack_name) do
    part = Partition.get_partition(stack_name)
    case Storage.get({:stack, :counter, stack_name}, part) do
      [] -> 0
      [{_, counter}] -> counter
    end
  end

  def get_all(stack_name) do
    part = Partition.get_partition(stack_name)
    to_list(part, stack_name)
  end

  ## private functions ##

  defp stack_push(partition, stack_name, value) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] -> # stack is not initialized
        case Storage.get({{:stack, :updating, stack_name}, :_}, partition) do
          [] -> # stack is not initialized
            stack_init(stack_name)
            stack_push(partition, stack_name, value)
          _ -> # stack is updating
            Process.sleep(0) # wait for stack is ready
            stack_push(partition, stack_name, value)
        end

      [{_, counter}] ->
        next_counter = counter + 1
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        Storage.put({{:stack, :counter, stack_name}, next_counter}, partition)
        Storage.put({{:stack, stack_name, next_counter}, value}, partition)

        Storage.delete({:stack, :updating, stack_name}, partition)
        Logger.debug("super_cache, stack, push value: #{inspect value} to stack: #{inspect stack_name}")
        true
    end
  end

  defp stack_pop(partition, stack_name, default) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] -> # stack is not initialized
        case Storage.get({{:stack, :updating, stack_name}, :_}, partition) do
          [] -> # stack is not initialized
            default
          _ -> # stack is updating
            Process.sleep(0) # wait for stack is ready
            stack_pop(partition, stack_name, default)
        end

      [{_, 0}]  ->
          default

      [{_, counter}] ->
        next_counter = counter - 1
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        value =
          case Storage.take({:stack, stack_name, counter}, partition) do
            [] ->
              Storage.put({{:stack, :counter, stack_name}, 0}, partition)
              default
            [{_, value}] ->
              Storage.delete({:stack, stack_name, counter}, partition)
              Storage.put({{:stack, :counter, stack_name}, next_counter}, partition)

              value
          end

        Storage.delete({:stack, :updating, stack_name}, partition)
        Logger.debug("super_cache, stack, push value: #{inspect value} to stack: #{inspect stack_name}")

        value

    end
  end

  defp stack_init(stack_name) do
    Logger.debug("super_cache, stack, init stack: #{inspect stack_name}")
    partition = Partition.get_partition(stack_name)

    Storage.put({{:stack, :counter, stack_name}, 0}, partition)
  end

  defp to_list(partition, stack_name) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] -> # stack is not initialized
        case Storage.get({{:stack, :updating, stack_name}, :_}, partition) do
          [] -> # stack is not initialized
            []
          _ -> # stack is updating
            Process.sleep(0) # wait for stack is ready
            to_list(partition, stack_name)
        end

      [{_, 0}]  ->
          []

      [{_, counter}] ->
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        value =
          Enum.reduce(counter..1//-1, [], fn x, acc ->
            case Storage.take({:stack, stack_name, x}, partition) do
              [] -> acc
              [{_, value}] -> [value | acc]
            end
          end)
          Storage.put({{:stack, :counter, stack_name}, 0}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)
        Logger.debug("super_cache, stack, push value: #{inspect value} to stack: #{inspect stack_name}")

        value

    end

  end

end
