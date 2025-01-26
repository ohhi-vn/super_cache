defmodule SuperCache.QueueTest do
  use ExUnit.Case
  doctest SuperCache

  alias SuperCache.Queue

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

  test "add & get data" do
    data = "Hello"
    Queue.add("my_queue", data)
    result = Queue.out("my_queue")
    assert data == result
  end

  test "get no data" do
    data = "Hello"
    result = Queue.out("my_queue", data)
    assert data == result
  end

  test "get data in empty queue" do
    data = "Hello"
    Queue.add("my_queue", data)
    Queue.out("my_queue")
    result = Queue.out("my_queue", nil)
    assert nil == result
  end


  test "order of queue" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x -> Queue.add("my_queue", x) end)
    result =
      Enum.reduce(1..10, true, fn x, acc ->
        acc and (Queue.out("my_queue") == x)
      end)

    assert true == result
  end

  test "order of multi queue" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x ->
      Queue.add("q1", x)
      Queue.add("q2", x)
      Queue.add("q3", x)
      Queue.add("q4", x)
      Queue.add("q5", x)
    end)
    result =
      Enum.reduce(1..10, true, fn x, acc ->
        acc and
        (Queue.out("q1") == x) and
        (Queue.out("q2") == x) and
        (Queue.out("q3") == x) and
        (Queue.out("q4") == x) and
        (Queue.out("q5") == x)
      end)

    assert true == result
  end
end
