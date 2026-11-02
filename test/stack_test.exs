defmodule SuperCache.StackTest do
  use ExUnit.Case, async: false
  doctest SuperCache

  alias SuperCache.Stack

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!()
    :ok
  end

  setup do
    SuperCache.delete_all()
  end

  test "push & pop data" do
    data = "Hello"
    Stack.push("my", data)
    result = Stack.pop("my")
    assert data == result
  end

  test "pop no Stack" do
    data = "Hello, no data"
    result = Stack.pop("my_stack", data)
    assert data == result
  end

  test "pop no data" do
    data = "Hello, no data"
    Stack.pop("my_stack")
    result = Stack.pop("my_stack", data)
    assert data == result
  end

  test "get data in empty stack" do
    data = "Hello"
    Stack.push("my", :hello)
    Stack.pop("my_stack", data)
    result = Stack.pop("my_stack", nil)
    assert nil == result
  end

  test "order of stack" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x -> Stack.push("my_stack", x) end)

    result =
      Enum.reduce(10..1//-1, true, fn x, acc ->
        acc and Stack.pop("my_stack") == x
      end)

    assert true == result
  end

  test "order of multi stack" do
    list = Enum.to_list(1..10)

    Enum.each(list, fn x ->
      Stack.push("s1", x)
      Stack.push("s2", x)
      Stack.push("s3", x)
      Stack.push("s4", x)
      Stack.push("s5", x)
    end)

    result =
      Enum.reduce(10..1//-1, true, fn x, acc ->
        acc and
          Stack.pop("s1") == x and
          Stack.pop("s2") == x and
          Stack.pop("s3") == x and
          Stack.pop("s4") == x and
          Stack.pop("s5") == x
      end)

    assert true == result
  end

  test "get all items" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x -> Stack.push("my_stack", x) end)
    result = Stack.get_all("my_stack")
    assert Enum.count(result) == 10
  end

  test "count item" do
    Enum.each(1..10, fn i -> Stack.push("my_stack", i) end)
    result = Stack.count("my_stack")
    assert 10 == result
  end

  test "get all itmes in empty stack" do
    result = Stack.get_all("my_stack")
    assert Enum.empty?(result)
  end

  test "mix operations in stack" do
    Stack.push("my_stack", 1)
    Stack.push("my_stack", 2)
    Stack.push("my_stack", 3)
    result = Stack.pop("my_stack")
    assert result == 3

    Stack.push("my_stack", 3)
    Stack.push("my_stack", 4)
    result = Stack.pop("my_stack")
    assert result == 4

    Stack.push("my_stack", 4)
    Stack.push("my_stack", 5)
    result = Stack.count("my_stack")
    assert result == 5

    result = Stack.get_all("my_stack")
    assert Enum.count(result) == 5
    assert 0 == Stack.count("my_stack")

    Enum.each(1..10, fn i -> Stack.push("my_stack", i) end)
    result = Stack.get_all("my_stack")
    assert Enum.count(result) == 10
  end

  test "concurrent pushes to a new stack retain every item" do
    count = 50

    1..count
    |> Enum.map(fn i -> Task.async(fn -> Stack.push("concurrent_stack", i) end) end)
    |> Enum.each(&Task.await(&1, 10_000))

    assert count == Stack.count("concurrent_stack")
    assert Enum.sort(Stack.get_all("concurrent_stack")) == Enum.to_list(1..count)
  end

  test "empty pop followed by push keeps the stack usable" do
    assert nil == Stack.pop("empty_then_push")
    assert 0 == Stack.count("empty_then_push")
    assert [] == Stack.get_all("empty_then_push")

    assert true == Stack.push("empty_then_push", :first)
    assert 1 == Stack.count("empty_then_push")
    assert :first == Stack.pop("empty_then_push")
    assert 0 == Stack.count("empty_then_push")
  end

  test "exhausted spin retries return the bounded default instead of spinning" do
    partition = SuperCache.Partition.get_partition("locked_stack")
    key = {:stack, :updating, "locked_stack"}
    # A live holder with a fresh timestamp is not reclaimable.
    SuperCache.Storage.put({key, {self(), System.monotonic_time(:millisecond)}}, partition)

    assert false == Stack.push("locked_stack", 1)
    assert :default == Stack.pop("locked_stack", :default)
    assert [] == Stack.get_all("locked_stack")
  end

  test "stale lock left by a dead process is reclaimed" do
    partition = SuperCache.Partition.get_partition("stale_stack")
    key = {:stack, :updating, "stale_stack"}
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, _, _, _}

    SuperCache.Storage.put(
      {key, {dead, System.monotonic_time(:millisecond) - 60_000}},
      partition
    )

    assert true == Stack.push("stale_stack", :recovered)
    assert :recovered == Stack.pop("stale_stack")
  end
end
