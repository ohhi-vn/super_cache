defmodule SuperCache.MetricsTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.Metrics

  @ns :metrics_test_ns

  # push_latency/1 is fire-and-forget (cast) — wait until the GenServer has
  # applied our writes before asserting.
  defp settle, do: Process.sleep(30)

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

  test "push_latency/2 stores samples newest-first" do
    Metrics.push_latency({@ns, :op}, 100)
    Metrics.push_latency({@ns, :op}, 200)
    Metrics.push_latency({@ns, :op}, 300)
    settle()

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

    wait_until(fn -> length(Metrics.get_latency_samples({@ns, :capped})) == 256 end, 50)
    samples = Metrics.get_latency_samples({@ns, :capped})

    assert hd(samples) == 300
    refute 1 in samples
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

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(20) && wait_until(fun, tries - 1)
  end
end
