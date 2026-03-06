defmodule SuperCache.Distributed.QueueTest do
  use ExUnit.Case, async: false

  alias SuperCache.Distributed.Queue

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

  ## add / out ##

  test "add and out single item" do
    Queue.add("q", "hello")
    assert "hello" == Queue.out("q")
  end

  test "out returns default when queue is empty" do
    assert nil == Queue.out("q")
    assert :empty == Queue.out("q", :empty)
  end

  test "out returns default when queue does not exist" do
    assert nil == Queue.out("no_such_queue")
  end

  test "FIFO order is maintained" do
    Enum.each(1..10, &Queue.add("q", &1))
    results = Enum.map(1..10, fn _ -> Queue.out("q") end)
    assert Enum.to_list(1..10) == results
  end

  test "interleaved add and out preserves order" do
    Queue.add("q", 1)
    Queue.add("q", 2)
    assert 1 == Queue.out("q")
    Queue.add("q", 3)
    assert 2 == Queue.out("q")
    assert 3 == Queue.out("q")
    assert nil == Queue.out("q")
  end

  ## multiple queues ##

  test "multiple queues are independent" do
    Enum.each(1..5, fn x ->
      Queue.add("qa", x)
      Queue.add("qb", x * 10)
    end)

    result =
      Enum.reduce(1..5, true, fn x, acc ->
        acc and Queue.out("qa") == x and Queue.out("qb") == x * 10
      end)

    assert result
  end

  ## peak ##

  test "peak returns front without removing" do
    Queue.add("q", :first)
    Queue.add("q", :second)
    assert :first == Queue.peak("q")
    assert :first == Queue.out("q")
    assert :second == Queue.out("q")
  end

  test "peak returns default on empty queue" do
    assert nil == Queue.peak("q")
    assert :none == Queue.peak("q", :none)
  end

  ## count ##

  test "count reflects queue size" do
    Enum.each(1..10, &Queue.add("q", &1))
    assert 10 == Queue.count("q")
  end

  test "count returns 0 for empty queue" do
    assert 0 == Queue.count("q")
  end

  test "count returns 0 for non-existent queue" do
    assert 0 == Queue.count("ghost_queue")
  end

  test "count decreases after out" do
    Enum.each(1..10, &Queue.add("q", &1))
    Queue.out("q")
    assert 9 == Queue.count("q")
  end

  test "count after mixed operations" do
    Enum.each(1..10, &Queue.add("q", &1))
    Queue.out("q")
    Queue.out("q")
    Queue.add("q", 99)
    Queue.out("q")
    assert 8 == Queue.count("q")
  end

  ## get_all (drain) ##

  test "get_all returns all items and empties the queue" do
    list = Enum.to_list(1..10)
    Enum.each(list, &Queue.add("q", &1))
    assert list == Queue.get_all("q")
    assert 0 == Queue.count("q")
  end

  test "get_all on empty queue returns empty list" do
    assert [] == Queue.get_all("q")
  end

  test "queue is usable after get_all" do
    Enum.each(1..5, &Queue.add("q", &1))
    Queue.get_all("q")

    Enum.each(1..3, &Queue.add("q", &1))
    assert 3 == Queue.count("q")
    assert 1 == Queue.out("q")
  end

  ## complex ##

  test "complex lifecycle" do
    list = Enum.to_list(1..10)
    Enum.each(list, &Queue.add("q", &1))
    assert list == Queue.get_all("q")

    Enum.each(1..5, &Queue.add("q", &1))
    assert 5 == Queue.count("q")

    Enum.each(1..5, fn _ -> Queue.out("q") end)
    assert 0 == Queue.count("q")

    Queue.add("q", 42)
    assert 1 == Queue.count("q")
    assert 42 == Queue.out("q")
    assert 0 == Queue.count("q")
  end
end
