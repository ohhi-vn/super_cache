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
end
