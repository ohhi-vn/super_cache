defmodule SuperCache.StackTest do
  use ExUnit.Case
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
        acc and (Stack.pop("my_stack") == x)
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
        (Stack.pop("s1") == x) and
        (Stack.pop("s2") == x) and
        (Stack.pop("s3") == x) and
        (Stack.pop("s4") == x) and
        (Stack.pop("s5") == x)
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
    assert Enum.count(result) == 0
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
end
