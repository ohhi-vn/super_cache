defmodule SuperCache.MetricsTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Metrics, Stats}

  @ns :metrics_test_ns

  # Counters are atomic ETS updates; latency samples are caller-written.
  defp settle, do: Process.sleep(10)

  setup do
    # Full isolation — the table also holds live system counters.
    :ets.delete_all_objects(Metrics)
    settle()
    :ok
  end

  test "increment/2 creates counters at zero and accumulates" do
    assert 1 == Metrics.increment(@ns, :calls)

    Metrics.increment(@ns, :calls)
    Metrics.increment(@ns, :errors)

    counts = Metrics.get_all(@ns)
    assert counts.calls == 2
    assert counts.errors == 1
  end

  test "get_all/1 returns empty map for unknown namespace" do
    assert %{} == Metrics.get_all(:never_used_ns_xyz)
  end

  test "increment works with tuple namespaces like {:api, :op}" do
    Metrics.increment({@ns, :put}, :calls)
    Metrics.increment({@ns, :put}, :calls)

    assert %{calls: 2} == Metrics.get_all({@ns, :put})
  end

  test "push_latency/2 stores samples newest-first and is synchronous" do
    assert :ok = Metrics.push_latency({@ns, :op}, 100)
    assert [100] == Metrics.get_latency_samples({@ns, :op})

    Metrics.push_latency({@ns, :op}, 200)
    Metrics.push_latency({@ns, :op}, 300)

    # No GenServer cast or settle delay: the caller already wrote the samples.
    assert [300, 200, 100] == Metrics.get_latency_samples({@ns, :op})
  end

  test "get_latency_samples/1 returns [] for unknown key" do
    assert [] == Metrics.get_latency_samples({@ns, :nope})
  end

  test "push_latency/2 rejects non-integer values" do
    assert_raise FunctionClauseError, fn ->
      Metrics.push_latency({@ns, :bad}, "not an int")
    end
  end

  test "latency ring buffer is capped at 256 newest samples" do
    for i <- 1..300, do: Metrics.push_latency({@ns, :capped}, i)

    samples = Metrics.get_latency_samples({@ns, :capped})

    assert length(samples) == 256
    assert hd(samples) == 300
    refute 1 in samples
  end

  test "concurrent writers keep the ring buffer bounded" do
    writers =
      for worker <- 1..8 do
        Task.async(fn ->
          for i <- 1..100 do
            Metrics.push_latency({@ns, :concurrent}, worker * 1_000 + i)
          end

          :ok
        end)
      end

    assert Enum.all?(Task.await_many(writers, 10_000), &(&1 == :ok))

    samples = Metrics.get_latency_samples({@ns, :concurrent})

    assert length(samples) == 256
    assert Enum.all?(samples, &is_integer/1)
    assert length(Enum.uniq(samples)) == length(samples)
  end

  test "latency reads do not depend on the GenServer processing messages" do
    :ok = :sys.suspend(Metrics)

    try do
      Metrics.push_latency({@ns, :suspended}, 42)
      assert [42] == Metrics.get_latency_samples({@ns, :suspended})
    after
      :sys.resume(Metrics)
    end
  end

  test "reliability counters increase and appear in Stats" do
    reliability = Stats.reliability()
    overview = Stats.cluster()

    assert Map.has_key?(overview, :reliability)
    assert overview.reliability == reliability

    before = reliability

    Metrics.increment(:replication, :async_failures)
    Metrics.increment(:replication, :async_drops)
    Metrics.increment(:replication, :sync_failures)
    Metrics.increment(:replication, :strong_failures)
    Metrics.increment(:wal, :backpressure)
    Metrics.increment(:read, :fallbacks)
    Metrics.increment(:health, :timeouts)
    Metrics.increment(:membership, :changes)
    Metrics.increment(:handoff, :successes)
    Metrics.increment(:handoff, :failures)
    Metrics.increment(:handoff, :transfer_failures)

    after_reliability = Stats.reliability()

    assert after_reliability.async_failures == before.async_failures + 1
    assert after_reliability.async_drops == before.async_drops + 1
    assert after_reliability.sync_failures == before.sync_failures + 1
    assert after_reliability.strong_failures == before.strong_failures + 1
    assert after_reliability.wal_backpressure == before.wal_backpressure + 1
    assert after_reliability.read_fallbacks == before.read_fallbacks + 1
    assert after_reliability.health_timeouts == before.health_timeouts + 1
    assert after_reliability.membership_changes == before.membership_changes + 1
    assert after_reliability.handoff_successes == before.handoff_successes + 1
    assert after_reliability.handoff_failures == before.handoff_failures + 1
    assert after_reliability.transfer_failures == before.transfer_failures + 1
  end

  test "reset/1 clears counters and latency samples" do
    Metrics.increment(@ns, :calls)
    Metrics.push_latency({@ns, :op}, 5)
    settle()

    assert :ok = Metrics.reset(@ns)
    assert %{} == Metrics.get_all(@ns)
    assert [] == Metrics.get_latency_samples({@ns, :op})
  end

  test "handle_info swallows stray messages without crashing" do
    send(Metrics, :random_garbage)
    settle()
    assert Process.alive?(Process.whereis(Metrics))
  end
end
