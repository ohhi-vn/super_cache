
defmodule SuperCache.Stack do
  @moduledoc """
  Named LIFO stacks backed by SuperCache ETS partitions.

  Any number of independent stacks can coexist by using different
  `stack_name` values.  Any process in the VM can push and pop from the
  same stack by name.

  Requires `SuperCache.start!/1` to be called first.

  ## Concurrency model

  Stack mutations use the same soft write-lock mechanism as `SuperCache.Queue`:
  a `{:stack, :updating, stack_name}` sentinel record is inserted before a
  structural change and removed immediately after.  Concurrent callers spin
  on `:erlang.yield/0` and retry until the lock is cleared.

  ## Storage layout

  | ETS key                              | Value   | Purpose                |
  |--------------------------------------|---------|------------------------|
  | `{:stack, :counter, stack_name}`     | integer | Number of items        |
  | `{:stack, stack_name, index}`        | any     | Item at position index |

  Items are indexed `1..counter` (1-based).  The top of the stack is always
  at `counter`.

  ## Example

      alias SuperCache.Stack

      SuperCache.start!()

      Stack.push("history", :page_a)
      Stack.push("history", :page_b)
      Stack.push("history", :page_c)

      Stack.count("history")      # => 3
      Stack.pop("history")        # => :page_c
      Stack.pop("history")        # => :page_b

      Stack.get_all("history")    # => [:page_a]  (drains the stack)
      Stack.count("history")      # => 0

      Stack.pop("history")        # => nil
      Stack.pop("history", :done) # => :done

  ## Cluster mode

  For distributed deployments use `SuperCache.Distributed.Stack`.  The API
  is identical; mutations are routed to the primary node for the stack's
  partition.
  """

  alias SuperCache.{Storage, Partition}
  require Logger

  ## API ────────────────────────────────────────────────────────────────────────

  @doc """
  Push `value` onto `stack_name`.

  Creates the stack if it does not yet exist.  Returns `true`.

  ## Example

      Stack.push("undo", {:insert, "hello"})
      Stack.push("undo", {:delete, 5..10})
  """
  @spec push(any, any) :: true
  def push(stack_name, value) do
    part = Partition.get_partition(stack_name)
    stack_push(part, stack_name, value)
  end

  @doc """
  Pop and return the top value from `stack_name`.

  Returns `default` (default `nil`) when the stack is empty or does not
  exist.

  ## Example

      Stack.push("s", :a)
      Stack.push("s", :b)
      Stack.pop("s")          # => :b
      Stack.pop("s")          # => :a
      Stack.pop("s")          # => nil
      Stack.pop("s", :empty)  # => :empty
  """
  @spec pop(any, any) :: any
  def pop(stack_name, default \\ nil) do
    part = Partition.get_partition(stack_name)
    stack_pop(part, stack_name, default)
  end

  @doc """
  Return the number of items in `stack_name`.

  Returns `0` for an empty or non-existent stack.

  ## Example

      Stack.push("s", 1)
      Stack.push("s", 2)
      Stack.count("s")   # => 2
  """
  @spec count(any) :: non_neg_integer
  def count(stack_name) do
    part = Partition.get_partition(stack_name)
    case Storage.get({:stack, :counter, stack_name}, part) do
      []             -> 0
      [{_, counter}] -> counter
    end
  end

  @doc """
  Remove and return **all** items top-first as a list.

  The stack is empty after this call.  Returns `[]` for an empty or
  non-existent stack.

  ## Example

      Enum.each(1..3, &Stack.push("s", &1))
      Stack.get_all("s")    # => [3, 2, 1]
      Stack.count("s")      # => 0
  """
  @spec get_all(any) :: list
  def get_all(stack_name) do
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
        Logger.debug(fn -> "super_cache, stack #{inspect(stack_name)}, push: #{inspect(value)}" end)
        true
    end
  end

  ## Private — pop ─────────────────────────────────────────────────────────────

  defp stack_pop(partition, stack_name, default) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({{:stack, :updating, stack_name}, :_}, partition) do
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
              default

            [{_, v}] ->
              Storage.delete({:stack, stack_name, counter}, partition)
              Storage.put({{:stack, :counter, stack_name}, next}, partition)
              v
          end

        Storage.delete({:stack, :updating, stack_name}, partition)
        Logger.debug(fn -> "super_cache, stack #{inspect(stack_name)}, pop: #{inspect(value)}" end)
        value
    end
  end

  ## Private — to_list (destructive drain) ─────────────────────────────────────

  defp to_list(partition, stack_name) do
    case Storage.take({:stack, :counter, stack_name}, partition) do
      [] ->
        case Storage.get({{:stack, :updating, stack_name}, :_}, partition) do
          [] -> []
          _  -> :erlang.yield(); to_list(partition, stack_name)
        end

      [{_, 0}] -> []

      [{_, counter}] ->
        Storage.put({{:stack, :updating, stack_name}, true}, partition)

        values =
          Enum.reduce(counter..1//-1, [], fn x, acc ->
            case Storage.take({:stack, stack_name, x}, partition) do
              []       -> acc
              [{_, v}] -> [v | acc]
            end
          end)

        Storage.put({{:stack, :counter, stack_name}, 0}, partition)
        Storage.delete({:stack, :updating, stack_name}, partition)
        Logger.debug(fn -> "super_cache, stack #{inspect(stack_name)}, drained #{length(values)} item(s)" end)
        values
    end
  end

  ## Private — init ────────────────────────────────────────────────────────────

  defp stack_init(stack_name) do
    Logger.debug("super_cache, stack, init: #{inspect(stack_name)}")
    partition = Partition.get_partition(stack_name)
    Storage.put({{:stack, :counter, stack_name}, 0}, partition)
  end
end
