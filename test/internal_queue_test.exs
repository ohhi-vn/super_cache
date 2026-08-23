defmodule SuperCache.Internal.QueueTest do
  use ExUnit.Case, async: false

  alias SuperCache.Internal.Queue

  @name :"TestQueue.InternalQueueTest"

  setup do
    case Process.whereis(@name) do
      nil -> :ok
      pid -> Queue.stop(pid)
    end

    Process.sleep(20)
    :ok
  end

  test "start/1 registers the name synchronously" do
    pid = Queue.start(@name)
    assert pid == Process.whereis(@name)
    assert Process.alive?(pid)

    Queue.stop(pid)
  end

  test "start/1 raises when the name is already taken" do
    _first = Queue.start(@name)
    assert_raise ArgumentError, fn -> Queue.start(@name) end
  end

  test "add/2 then get/1 returns buffered items" do
    pid = Queue.start(@name)
    Queue.add(pid, {:item, 1})
    Queue.add(pid, {:item, 2})

    assert items = Queue.get(pid)
    assert {:item, 1} in items
    assert {:item, 2} in items

    Queue.stop(pid)
  end

  test "get/1 blocks until data arrives" do
    pid = Queue.start(@name)

    test_pid = self()

    reader =
      spawn(fn ->
        send(test_pid, {:got, Queue.get(pid)})
      end)

    # No data yet — reader must still be waiting.
    Process.sleep(50)
    assert Process.alive?(reader)
    refute_received {:got, _}

    Queue.add(pid, :late_item)

    assert_receive {:got, [:late_item]}, 1_000
    Queue.stop(pid)
  end

  test "stop/1 notifies waiting readers with []" do
    pid = Queue.start(@name)
    test_pid = self()

    spawn(fn ->
      send(test_pid, {:got, Queue.get(pid)})
    end)

    Process.sleep(50)
    Queue.stop(pid)

    assert_receive {:got, []}, 1_000
    # Queue process must actually terminate (no zombies).
    Process.sleep(50)
    assert Queue.down?(pid)
  end

  test "get/1 returns [] for a dead queue instead of hanging" do
    pid = Queue.start(@name)
    Queue.stop(pid)

    Process.sleep(50)
    assert Queue.get(pid) == []
  end

  test "get/1 returns [] when the queue dies mid-wait" do
    pid = Queue.start(@name)
    test_pid = self()

    spawn(fn ->
      send(test_pid, {:got, Queue.get(pid)})
    end)

    Process.sleep(50)
    # Kill while reader is blocked.
    Process.exit(pid, :kill)

    assert_receive {:got, []}, 1_000
  end

  test "down?/1 detects dead queues by atom and pid" do
    pid = Queue.start(@name)
    refute Queue.down?(pid)
    refute Queue.down?(@name)

    Queue.stop(pid)
    Process.sleep(50)
    assert Queue.down?(@name)
    assert Queue.down?(pid)
  end

  test "down?/1 is true for never-started names" do
    assert Queue.down?(:"Never.Started.Queue")
  end
end
