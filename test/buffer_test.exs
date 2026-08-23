defmodule SuperCache.BufferTest do
  use ExUnit.Case, async: false

  alias SuperCache.Buffer

  require Logger

  setup_all do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
    Process.sleep(30)

    :ok
  end

  @buffer_1 :"SuperCache.Buffer_1"

  test "enqueue flushes buffered data into the cache" do
    SuperCache.delete_by_match!(:_, {:lazy_flush, :_, :_})

    assert :ok = Buffer.enqueue({:lazy_flush, :probe, 1})

    # Wait for the buffer stream to drain the queue.
    wait_until(fn -> SuperCache.get!({:lazy_flush, nil}) != [] end)
    assert [{:lazy_flush, :probe, 1}] == SuperCache.get!({:lazy_flush, nil})
  end

  test "enqueue returns an error when buffers are not started" do
    names = :persistent_term.get({Buffer, :buffer_names}, nil)

    try do
      :persistent_term.erase({Buffer, :buffer_names})
      assert {:error, :not_started} = Buffer.enqueue({:x, :y, :z})
    after
      if names, do: :persistent_term.put({Buffer, :buffer_names}, names)
    end
  end

  test "self-heals when the queue process is killed" do
    old_pid = Process.whereis(@buffer_1)
    assert old_pid

    # Simulate a crash — without self-healing the buffer would stay dead
    # forever and lazy_put/1 writes would be silently dropped.
    Process.exit(old_pid, :kill)

    # A replacement queue must come back under the same name.
    wait_until(fn ->
      case Process.whereis(@buffer_1) do
        nil -> false
        pid -> pid != old_pid
      end
    end)

    # And buffered writes must flow again.
    assert :ok = Buffer.enqueue({:heal_check, :after_kill, 1})
    wait_until(fn -> SuperCache.get!({:heal_check, nil}) != [] end)
  end

  test "stop/0 reports an accurate stopped count and marks buffers stopped" do
    names = :persistent_term.get({Buffer, :buffer_names}, nil)
    assert is_tuple(names)

    # test.exs caps the logger at :warning; temporarily open it up so the
    # info-level stop summary can be captured.
    orig_level = Logger.level()
    Logger.configure(level: :info)

    log =
      try do
        ExUnit.CaptureLog.capture_log(fn ->
          Buffer.stop()
        end)
      after
        Logger.configure(level: orig_level)
      end

    # Restore first so a failed assertion never leaks state into other tests.
    num = SuperCache.Partition.get_schedulers()
    Buffer.start(num)
    assert is_tuple(:persistent_term.get({Buffer, :buffer_names}, nil))

    assert log =~ ~r/stopped \d+\/\d+ buffer/
  end

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met within timeout")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end
end
