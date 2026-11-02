defmodule SuperCache.QueueTest do
  use ExUnit.Case, async: false
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
        acc and Queue.out("my_queue") == x
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
          Queue.out("q1") == x and
          Queue.out("q2") == x and
          Queue.out("q3") == x and
          Queue.out("q4") == x and
          Queue.out("q5") == x
      end)

    assert true == result
  end

  test "count item" do
    Enum.each(1..10, fn i -> Queue.add("my_queue", i) end)
    result = Queue.count("my_queue")
    assert 10 == result
  end

  test "count item after call out function" do
    Enum.each(1..10, fn i -> Queue.add("my_queue", i) end)
    Queue.out("my_queue")
    result = Queue.count("my_queue")
    assert 9 == result
  end

  test "count item after call functions" do
    Enum.each(1..10, fn i -> Queue.add("my_queue", i) end)
    Queue.out("my_queue")
    Queue.out("my_queue")
    Queue.add("my_queue", 1)
    Queue.out("my_queue")

    result = Queue.count("my_queue")
    assert 8 == result
  end

  test "count item in empty queue" do
    result = Queue.count("my_queue")
    assert 0 == result
  end

  test "count item in not existed queue" do
    result = Queue.count("not_existed_queue")
    assert 0 == result
  end

  test "peak item" do
    Enum.each(1..10, fn i -> Queue.add("my_queue", i) end)
    result = Queue.peak("my_queue")
    item = Queue.out("my_queue")
    assert item == result
  end

  test "peak item in empty queue" do
    result = Queue.peak("my_queue")
    assert nil == result
  end

  test "get all items" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x -> Queue.add("my_queue", x) end)
    result = Queue.get_all("my_queue")
    assert list == result
  end

  test "complex operations" do
    list = Enum.to_list(1..10)
    Enum.each(list, fn x -> Queue.add("my_queue", x) end)
    result = Queue.get_all("my_queue")
    assert list == result

    Enum.each(1..5, fn x -> Queue.add("my_queue", x) end)
    result = Queue.count("my_queue")
    assert 5 == result

    Enum.each(1..5, fn x -> Queue.out("my_queue") end)
    result = Queue.count("my_queue")
    assert 0 == result

    Queue.add("my_queue", 1)
    result = Queue.count("my_queue")
    assert 1 == result

    result = Queue.out("my_queue")
    assert 1 == result

    result = Queue.count("my_queue")
    assert 0 == result
  end

  test "concurrent enqueues to a new queue retain every item" do
    count = 50

    1..count
    |> Enum.map(fn i -> Task.async(fn -> Queue.add("concurrent_queue", i) end) end)
    |> Enum.each(&Task.await(&1, 10_000))

    assert count == Queue.count("concurrent_queue")
    assert Enum.sort(Queue.get_all("concurrent_queue")) == Enum.to_list(1..count)
  end

  test "lock held by a dead process is reclaimed" do
    partition = SuperCache.Partition.get_partition("stale_queue")
    key = {:queue, :updating, "stale_queue"}
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, _, _, _}

    SuperCache.Storage.put(
      {key, {dead, System.monotonic_time(:millisecond) - 60_000}},
      partition
    )

    assert true == Queue.add("stale_queue", :recovered)
    assert :recovered == Queue.out("stale_queue")
  end

  test "a live lock blocks mutations within the bounded retry budget" do
    partition = SuperCache.Partition.get_partition("locked_queue")
    key = {:queue, :updating, "locked_queue"}
    SuperCache.Storage.put({key, {self(), System.monotonic_time(:millisecond)}}, partition)

    assert false == Queue.add("locked_queue", 1)
    assert :default == Queue.out("locked_queue", :default)
    assert [] == Queue.get_all("locked_queue")
  end
end
