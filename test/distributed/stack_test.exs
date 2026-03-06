defmodule SuperCache.Distributed.StackTest do
  use ExUnit.Case, async: false

  alias SuperCache.Distributed.Stack

  setup_all do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(50)
    SuperCache.start!()
    :ok
  end

  setup do
    SuperCache.delete_all()
    :ok
  end

  ## push / pop ##

  test "push and pop single item" do
    Stack.push("s", "hello")
    assert "hello" == Stack.pop("s")
  end

  test "pop returns default when stack is empty" do
    assert nil == Stack.pop("s")
    assert :empty == Stack.pop("s", :empty)
  end

  test "pop returns default when stack does not exist" do
    assert nil == Stack.pop("no_such_stack")
  end

  test "LIFO order is maintained" do
    Enum.each(1..10, &Stack.push("s", &1))
    results = Enum.map(1..10, fn _ -> Stack.pop("s") end)
    assert Enum.to_list(10..1//-1) == results
  end

  test "interleaved push and pop preserves LIFO order" do
    Stack.push("s", 1)
    Stack.push("s", 2)
    assert 2 == Stack.pop("s")
    Stack.push("s", 3)
    assert 3 == Stack.pop("s")
    assert 1 == Stack.pop("s")
    assert nil == Stack.pop("s")
  end

  ## multiple stacks ##

  test "multiple stacks are independent" do
    Enum.each(1..5, fn x ->
      Stack.push("sa", x)
      Stack.push("sb", x * 10)
    end)

    result =
      Enum.reduce(5..1//-1, true, fn x, acc ->
        acc and Stack.pop("sa") == x and Stack.pop("sb") == x * 10
      end)

    assert result
  end

  ## count ##

  test "count reflects stack size" do
    Enum.each(1..10, &Stack.push("s", &1))
    assert 10 == Stack.count("s")
  end

  test "count returns 0 for empty stack" do
    assert 0 == Stack.count("s")
  end

  test "count returns 0 for non-existent stack" do
    assert 0 == Stack.count("ghost_stack")
  end

  test "count decreases after pop" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.pop("s")
    assert 4 == Stack.count("s")
  end

  test "count after mixed push and pop" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.pop("s")
    Stack.pop("s")
    Stack.push("s", 99)
    assert 4 == Stack.count("s")
  end

  ## get_all (drain) ##

  test "get_all returns items top-first and clears stack" do
    Enum.each(1..5, &Stack.push("s", &1))
    assert [5, 4, 3, 2, 1] == Stack.get_all("s")
    assert 0 == Stack.count("s")
  end

  test "get_all on empty stack returns empty list" do
    assert [] == Stack.get_all("s")
  end

  test "stack is usable after get_all" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.get_all("s")

    Stack.push("s", :new)
    assert 1 == Stack.count("s")
    assert :new == Stack.pop("s")
  end

  ## complex ##

  test "complex lifecycle" do
    Enum.each(1..10, &Stack.push("s", &1))
    assert 10 == Stack.count("s")

    top = Stack.pop("s")
    assert 10 == top
    assert 9 == Stack.count("s")

    Stack.push("s", 100)
    assert 10 == Stack.count("s")
    assert 100 == Stack.pop("s")

    all = Stack.get_all("s")
    assert length(all) == 9
    assert 0 == Stack.count("s")

    Stack.push("s", :after_drain)
    assert 1 == Stack.count("s")
    assert :after_drain == Stack.pop("s")
    assert 0 == Stack.count("s")
  end
end
