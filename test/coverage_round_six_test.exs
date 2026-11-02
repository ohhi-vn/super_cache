defmodule SuperCache.CoverageRoundSixTest do
  @moduledoc """
  Coverage round six — fault-injection paths that need process-level control:

  * `SuperCache.Buffer` / `SuperCache.Internal.Stream` — non-list stream items,
    graceful-shutdown timeout, restart-budget exhaustion
  * `SuperCache.Cluster.Replicator` — 500-record batch flush, successful
    multicall accounting, malformed-replica rescue, async retry/drop paths,
    unavailable worker pool
  """

  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Buffer, Config, Partition, Storage}
  alias SuperCache.Cluster.{Metrics, Replicator}
  alias SuperCache.Internal.Stream

  @moduletag :sequential
  @moduletag timeout: 120_000

  @buffer_pt_key {Buffer, :buffer_names}
  @partition_map_key {SuperCache.Cluster.Manager, :partition_map}

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup do
    restart_cache!()

    on_exit(fn ->
      restore_replication_mode()
      ensure_buffers!()
    end)

    :ok
  end

  defp local_opts, do: [key_pos: 0, partition_pos: 0, num_partition: 4]

  defp restart_cache!(opts \\ local_opts()) do
    try do
      if SuperCache.started?(), do: Bootstrap.stop()
    catch
      _, _ -> :ok
    end

    Process.sleep(50)
    Bootstrap.start!(opts)
    Process.sleep(50)
    :ok
  end

  defp restore_replication_mode do
    Config.set_config(:replication_mode, :async)
  catch
    _, _ -> :ok
  end

  # ── Buffer helpers ────────────────────────────────────────────────────────────

  defp buffer_names do
    case :persistent_term.get(@buffer_pt_key, nil) do
      names when is_tuple(names) -> Tuple.to_list(names)
      _ -> []
    end
  end

  defp ensure_buffers! do
    names = buffer_names()

    if names == [] or Enum.any?(names, &(Process.whereis(&1) == nil)) do
      Buffer.stop()
      Process.sleep(50)
      Buffer.start(:erlang.system_info(:schedulers_online))
      Process.sleep(50)
    end

    :ok
  catch
    _, _ -> :ok
  end

  defp wait_for_queue(name, timeout_ms) do
    wait_until(fn -> is_pid(Process.whereis(name)) end, timeout_ms)
    Process.whereis(name)
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition not met before the deadline")

      true ->
        Process.sleep(20)
        do_wait_until(fun, deadline)
    end
  end

  # ── Partition-map helpers ─────────────────────────────────────────────────────

  defp with_partition_map(overrides, fun) do
    old = :persistent_term.get(@partition_map_key, %{})
    :persistent_term.put(@partition_map_key, Map.merge(old, overrides))

    try do
      fun.()
    after
      :persistent_term.put(@partition_map_key, old)
    end
  end

  defp replication_metrics do
    Metrics.get_all(:replication)
  end

  defp metric(name) do
    Map.get(replication_metrics(), name, 0)
  end

  # ── Buffer ───────────────────────────────────────────────────────────────────

  describe "SuperCache.Buffer" do
    test "stop/0 waits out a queue that ignores :stop" do
      name = hd(buffer_names())
      assert is_pid(Process.whereis(name))

      assert :ok = Buffer.stop()
      wait_until(fn -> Process.whereis(name) == nil end, 2_000)

      dummy = spawn(fn -> ignore_messages() end)
      Process.register(dummy, name)

      assert :ok = Buffer.start(:erlang.system_info(:schedulers_online))
      Process.sleep(100)

      {elapsed_us, result} = :timer.tc(fn -> Buffer.stop() end)

      assert result == :ok
      assert elapsed_us >= 900_000

      Process.exit(dummy, :kill)
      wait_until(fn -> Process.whereis(name) == nil end, 2_000)

      assert :ok = Buffer.start(:erlang.system_info(:schedulers_online))
      assert wait_for_queue(name, 3_000)
    end

    test "rapid queue deaths exhaust the restart budget and log the give-up" do
      name = hd(buffer_names())
      assert is_pid(Process.whereis(name))

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          first = wait_for_queue(name, 2_000)
          Process.exit(first, :kill)
          wait_until(fn -> Process.whereis(name) == nil end, 2_000)

          Enum.each(2..11, fn restart ->
            timeout = min(250 * (restart - 1), 5_000) + 2_000
            pid = wait_for_queue(name, timeout)
            Process.exit(pid, :kill)
            wait_until(fn -> Process.whereis(name) == nil end, 2_000)
          end)

          # The runner logs the give-up synchronously after the last death.
          Process.sleep(300)
        end)

      assert log =~ "exceeded 10 consecutive restarts"
      assert Process.whereis(name) == nil

      assert :ok = Buffer.stop()
      assert :ok = Buffer.start(:erlang.system_info(:schedulers_online))
      assert wait_for_queue(name, 3_000)
    end
  end

  defp ignore_messages do
    receive do
      _ -> ignore_messages()
    end
  end

  # ── Stream ───────────────────────────────────────────────────────────────────

  describe "SuperCache.Internal.Stream" do
    test "make_stream_pipe/1 flushes a bare tuple item directly" do
      key = {:cov6_stream, System.unique_integer([:positive])}

      assert :ok = Stream.make_stream_pipe([{key, "value"}])
      assert [{^key, "value"}] = SuperCache.get!({key, nil})
    end
  end

  # ── Replicator ───────────────────────────────────────────────────────────────

  describe "SuperCache.Cluster.Replicator" do
    test "push_partition/2 flushes in 500-record batches" do
      partition = Partition.get_partition_by_idx(0)
      Storage.delete_all(partition)

      records = for i <- 1..501, do: {{:cov6_batch, i}, i}
      Storage.put(records, partition)

      assert :ok = Replicator.push_partition(0, node())

      Storage.delete_all(partition)
    end

    test "replicate_batch/3 counts successful multicall replies" do
      partition = Partition.get_partition_by_idx(2)
      Storage.delete_all(partition)

      with_partition_map(%{2 => {node(), [node()]}}, fn ->
        assert :ok = Replicator.replicate_batch(2, :put, [{:cov6_replicate_batch, 1}])
      end)

      assert [{:cov6_replicate_batch, 1}] = Storage.get(:cov6_replicate_batch, partition)

      Storage.delete_all(partition)
    end

    test "replicate_batch/3 rescues malformed replica lists" do
      with_partition_map(%{2 => {node(), :not_a_list}}, fn ->
        assert {:error, _reason} = Replicator.replicate_batch(2, :put, [{:cov6_bad, 1}])
      end)
    end

    test "replicate/3 async multicall handles the classic reply tuple" do
      Config.set_config(:replication_mode, :async)
      partition = Partition.get_partition_by_idx(2)
      Storage.delete_all(partition)

      with_partition_map(%{2 => {node(), [node()]}}, fn ->
        assert :ok = Replicator.replicate(2, :put, {:cov6_async_ok, 1})
      end)

      wait_until(
        fn -> Storage.get(:cov6_async_ok, partition) == [{:cov6_async_ok, 1}] end,
        2_000
      )

      Storage.delete_all(partition)
    end

    test "replicate/3 async retries and counts failures when multicall raises" do
      Config.set_config(:replication_mode, :async)
      before = metric(:async_failures)

      with_partition_map(%{2 => {node(), :not_a_list}}, fn ->
        assert :ok = Replicator.replicate(2, :put, {:cov6_async_retry, 1})
      end)

      wait_until(fn -> metric(:async_failures) > before end, 3_000)
    end

    test "replicate/3 drops async work when the pool is full" do
      Config.set_config(:replication_mode, :async)
      pool = SuperCache.Cluster.Replicator.Pool
      supervisor = SuperCache.Supervisor
      before = metric(:async_drops)

      assert :ok = Supervisor.terminate_child(supervisor, pool)
      {:ok, temp_pool} = Task.Supervisor.start_link(name: pool, max_children: 0)

      try do
        with_partition_map(%{2 => {node(), [node()]}}, fn ->
          assert :ok = Replicator.replicate(2, :put, {:cov6_pool_full, 1})
        end)

        assert metric(:async_drops) == before + 1
      after
        Supervisor.stop(temp_pool)
        assert {:ok, _pid} = Supervisor.restart_child(supervisor, pool)
      end
    end

    test "replicate/3 async counts drops after exhausting retries" do
      Config.set_config(:replication_mode, :async)
      before = metric(:async_drops)

      with_partition_map(%{2 => {node(), [:"cov6_missing@127.0.0.1"]}}, fn ->
        assert :ok = Replicator.replicate(2, :put, {:cov6_async_drop, 1})
      end)

      wait_until(fn -> metric(:async_drops) > before end, 5_000)
    end
  end
end
