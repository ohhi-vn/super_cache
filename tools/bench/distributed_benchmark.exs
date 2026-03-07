#!/usr/bin/env elixir
# =============================================================================
# SuperCache Distributed Benchmark
#
# Usage:
#   # Single node (baseline)
#   mix run tools/bench/distributed_benchmark.exs
#
#   # Multi-node — start peers first, then run on the coordinator node
#   elixir --sname coordinator --cookie secret -S mix run tools/bench/distributed_benchmark.exs
#
# Options (set as env vars):
#   BENCH_PEERS          comma-separated peer node names  (default: "")
#   BENCH_PARTITIONS     number of ETS partitions          (default: 8)
#   BENCH_REP_FACTOR     replication factor                (default: 2)
#   BENCH_REP_MODE       async | sync | strong             (default: async)
#   BENCH_DURATION_S     seconds per scenario              (default: 5)
#   BENCH_CONCURRENCY    number of parallel worker tasks   (default: 50)
#   BENCH_PAYLOAD_BYTES  approximate value payload size    (default: 64)
# =============================================================================

defmodule Bench.Config do
  def peers do
    System.get_env("BENCH_PEERS", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom/1)
  end

  def partitions,     do: env_int("BENCH_PARTITIONS", 8)
  def rep_factor,     do: env_int("BENCH_REP_FACTOR", 2)
  def rep_mode,       do: System.get_env("BENCH_REP_MODE", "strong") |> String.to_atom()
  def duration_ms,    do: env_int("BENCH_DURATION_S", 5) * 1_000
  def concurrency,    do: env_int("BENCH_CONCURRENCY", 50)
  def payload_bytes,  do: env_int("BENCH_PAYLOAD_BYTES", 64)

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      v   -> String.to_integer(v)
    end
  end
end

defmodule Bench.Payload do
  @chars "abcdefghijklmnopqrstuvwxyz0123456789" |> String.graphemes()

  def generate(bytes) do
    Stream.repeatedly(fn -> Enum.random(@chars) end)
    |> Enum.take(bytes)
    |> Enum.join()
  end

  # Pre-generate a pool of payloads to avoid alloc noise in hot loops.
  def pool(size, bytes) do
    Enum.map(1..size, fn _ -> generate(bytes) end) |> List.to_tuple()
  end

  def pick(pool) do
    elem(pool, :rand.uniform(tuple_size(pool)) - 1)
  end
end

defmodule Bench.Reporter do
  def print_header(title) do
    bar = String.duplicate("─", 66)
    IO.puts("\n╔#{String.duplicate("═", 66)}╗")
    IO.puts("║#{String.pad_leading(title, div(66 + String.length(title), 2)) |> String.pad_trailing(66)}║")
    IO.puts("╚#{String.duplicate("═", 66)}╝")
  end

  def print_result(label, result) do
    %{
      ops:        ops,
      errors:     errors,
      duration_ms: dur,
      latencies:  lats
    } = result

    throughput = Float.round(ops / (dur / 1_000), 1)
    sorted     = Enum.sort(lats)
    count      = length(sorted)

    {avg, p50, p95, p99, p999, min_l, max_l} =
      if count == 0 do
        {0, 0, 0, 0, 0, 0, 0}
      else
        avg   = div(Enum.sum(sorted), count)
        p50   = percentile(sorted, count, 0.50)
        p95   = percentile(sorted, count, 0.95)
        p99   = percentile(sorted, count, 0.99)
        p999  = percentile(sorted, count, 0.999)
        {avg, p50, p95, p99, p999, hd(sorted), List.last(sorted)}
      end

    IO.puts("""
    ┌─ #{label}
    │  ops=#{ops}  errors=#{errors}  duration=#{dur}ms  throughput=#{throughput} ops/s
    │  latency (µs):
    │    min=#{min_l}  avg=#{avg}  p50=#{p50}  p95=#{p95}  p99=#{p99}  p99.9=#{p999}  max=#{max_l}
    └#{String.duplicate("─", 64)}
    """)
  end

  defp percentile(sorted, count, pct) do
    idx = min(trunc(count * pct), count - 1)
    Enum.at(sorted, idx)
  end

  def print_comparison(results) do
    IO.puts("\n  Scenario comparison (throughput ops/s):")
    IO.puts("  #{String.duplicate("─", 50)}")

    max_tp =
      results
      |> Enum.map(fn {_, r} -> r.ops / (r.duration_ms / 1_000) end)
      |> Enum.max()

    Enum.each(results, fn {label, r} ->
      tp    = r.ops / (r.duration_ms / 1_000)
      bar_w = if max_tp > 0, do: trunc(tp / max_tp * 30), else: 0
      bar   = String.duplicate("█", bar_w) |> String.pad_trailing(30)
      tp_s  = Float.round(tp, 1) |> to_string() |> String.pad_leading(10)
      IO.puts("  #{String.pad_trailing(label, 18)} #{bar} #{tp_s}")
    end)
  end

  def print_cluster_stats do
    alias SuperCache.Cluster.Stats
    IO.puts("\n── Cluster Stats ──────────────────────────────────────────────────")
    stats = Stats.cluster()
    IO.puts("  nodes:           #{inspect(stats.nodes)}")
    IO.puts("  partitions:      #{stats.num_partitions}")
    IO.puts("  rep_factor:      #{stats.replication_factor}")
    IO.puts("  rep_mode:        #{stats.replication_mode}")
    IO.puts("  total_records:   #{stats.total_records}")

    primary = Stats.primary_partitions()
    replica = Stats.replica_partitions()
    IO.puts("  this node:       #{length(primary)} primary, #{length(replica)} replica partitions")

    IO.puts("\n── 3PC Stats ───────────────────────────────────────────────────────")
    tpc = Stats.three_phase_commit()
    IO.puts("  in_flight:            #{tpc.in_flight}")
    IO.puts("  committed:            #{tpc.committed}")
    IO.puts("  aborted:              #{tpc.aborted}")
    IO.puts("  prepare_failures:     #{tpc.prepare_failures}")
    IO.puts("  pre_commit_failures:  #{tpc.pre_commit_failures}")
    IO.puts("  commit_failures:      #{tpc.commit_failures}")
    IO.puts("  avg_commit_us:        #{tpc.avg_commit_latency_us}")
    IO.puts("  p99_commit_us:        #{tpc.p99_commit_latency_us}")

    IO.puts("\n── API Stats ───────────────────────────────────────────────────────")
    api = Stats.api()

    Enum.each(api, fn {op, m} ->
      op_s     = op     |> to_string() |> String.pad_trailing(20)
      calls_s  = m.calls  |> to_string() |> String.pad_trailing(8)
      errors_s = m.errors |> to_string() |> String.pad_trailing(6)
      avg_s    = m.avg_us |> to_string() |> String.pad_trailing(8)
      p99_s    = m.p99_us |> to_string() |> String.pad_trailing(8)
      IO.puts("  #{op_s}  calls=#{calls_s}  errors=#{errors_s}  avg_us=#{avg_s}  p99_us=#{p99_s}")
    end)
  end
end

defmodule Bench.Worker do
  @doc """
  Run `fun/0` as fast as possible for `duration_ms`, using `concurrency`
  parallel tasks.  Returns a result map with ops, errors, and raw latency
  samples (one per op, µs).
  """
  def run(fun, duration_ms, concurrency) do
    parent   = self()
    deadline = System.monotonic_time(:millisecond) + duration_ms

    tasks =
      Enum.map(1..concurrency, fn _ ->
        Task.async(fn ->
          worker_loop(fun, deadline, 0, 0, [])
          |> tap(fn r -> send(parent, {:worker_done, r}) end)
        end)
      end)

    Task.await_many(tasks, duration_ms + 10_000)

    results =
      Enum.map(tasks, fn _ ->
        receive do
          {:worker_done, r} -> r
        after
          15_000 -> %{ops: 0, errors: 0, latencies: []}
        end
      end)

    %{
      ops:         Enum.sum(Enum.map(results, & &1.ops)),
      errors:      Enum.sum(Enum.map(results, & &1.errors)),
      duration_ms: duration_ms,
      latencies:   Enum.flat_map(results, & &1.latencies)
    }
  end

  defp worker_loop(fun, deadline, ops, errors, lats) do
    if System.monotonic_time(:millisecond) >= deadline do
      %{ops: ops, errors: errors, latencies: lats}
    else
      t0 = System.monotonic_time(:microsecond)

      {ok, _} =
        try do
          result = fun.()
          {:ok, result}
        rescue
          _ -> {:error, nil}
        end

      elapsed = System.monotonic_time(:microsecond) - t0
      new_errors = if ok == :error, do: errors + 1, else: errors

      # Keep at most 10k samples per worker to bound memory.
      new_lats =
        if length(lats) < 10_000 do
          [elapsed | lats]
        else
          lats
        end

      worker_loop(fun, deadline, ops + 1, new_errors, new_lats)
    end
  end
end

defmodule Bench.Scenarios do
  alias SuperCache.Distributed, as: Cache

  def write_only(pool, key_range) do
    fn ->
      key     = :rand.uniform(key_range)
      payload = Bench.Payload.pick(pool)
      Cache.put!({:bench, key, payload})
    end
  end

  def read_only_local(key_range) do
    fn ->
      key = :rand.uniform(key_range)
      Cache.get!({:bench, key, nil})
    end
  end

  def read_only_primary(key_range) do
    fn ->
      key = :rand.uniform(key_range)
      Cache.get!({:bench, key, nil}, read_mode: :primary)
    end
  end

  def read_only_quorum(key_range) do
    fn ->
      key = :rand.uniform(key_range)
      Cache.get!({:bench, key, nil}, read_mode: :quorum)
    end
  end

  def read_write_mixed(pool, key_range, write_pct \\ 20) do
    fn ->
      if :rand.uniform(100) <= write_pct do
        key     = :rand.uniform(key_range)
        payload = Bench.Payload.pick(pool)
        Cache.put!({:bench, key, payload})
      else
        Cache.get!({:bench, :rand.uniform(key_range), nil})
      end
    end
  end

  def delete_mixed(pool, key_range) do
    fn ->
      key = :rand.uniform(key_range)
      r   = :rand.uniform(3)

      cond do
        r == 1 ->
          payload = Bench.Payload.pick(pool)
          Cache.put!({:bench, key, payload})
        r == 2 ->
          Cache.delete!({:bench, key, nil})
        true ->
          Cache.get!({:bench, key, nil})
      end
    end
  end

  def kv_write(key_range) do
    fn ->
      key = :rand.uniform(key_range)
      SuperCache.Distributed.KeyValue.add("bench_kv", key, key * 2)
    end
  end

  def kv_read(key_range) do
    fn ->
      key = :rand.uniform(key_range)
      SuperCache.Distributed.KeyValue.get("bench_kv", key)
    end
  end

  def queue_throughput do
    fn ->
      if :rand.uniform(2) == 1 do
        SuperCache.Distributed.Queue.add("bench_q", :rand.uniform(1_000_000))
      else
        SuperCache.Distributed.Queue.out("bench_q")
      end
    end
  end

  def stack_throughput do
    fn ->
      if :rand.uniform(2) == 1 do
        SuperCache.Distributed.Stack.push("bench_stack", :rand.uniform(1_000_000))
      else
        SuperCache.Distributed.Stack.pop("bench_stack")
      end
    end
  end
end

# =============================================================================
# Bootstrap
# =============================================================================

defmodule Bench.Runner do
  def run do
    cfg = Bench.Config

    Bench.Reporter.print_header("SuperCache Distributed Benchmark")

    IO.puts("""

    Configuration:
      peers         : #{inspect(cfg.peers())}
      partitions    : #{cfg.partitions()}
      rep_factor    : #{cfg.rep_factor()}
      rep_mode      : #{cfg.rep_mode()}
      duration      : #{div(cfg.duration_ms(), 1_000)}s per scenario
      concurrency   : #{cfg.concurrency()} workers
      payload       : #{cfg.payload_bytes()} bytes
    """)

    # ── Start SuperCache ────────────────────────────────────────────────────
    if SuperCache.started?(), do: SuperCache.Cluster.Bootstrap.stop()
    Process.sleep(100)

    SuperCache.Cluster.Bootstrap.start!(
      key_pos:            0,
      partition_pos:      0,
      cluster:            :distributed,
      replication_factor: cfg.rep_factor(),
      replication_mode:   cfg.rep_mode(),
      num_partition:      cfg.partitions(),
      table_type:         :set
    )

    # ── Connect peers ───────────────────────────────────────────────────────
    Enum.each(cfg.peers(), fn peer ->
      case Node.connect(peer) do
        true  -> IO.puts("  connected → #{peer}")
        false -> IO.puts("  WARN: could not connect to #{peer}")
        :ignored -> IO.puts("  WARN: node not alive #{peer}")
      end
    end)

    if cfg.peers() != [] do
      IO.puts("  waiting for cluster to stabilise…")
      Process.sleep(1_500)
    end

    # ── Pre-warm: seed 10k keys so reads aren't all misses ─────────────────
    IO.puts("\n  pre-warming 10 000 keys…")
    pool = Bench.Payload.pool(200, cfg.payload_bytes())

    Enum.each(1..10_000, fn i ->
      SuperCache.Distributed.put!({:bench, i, Bench.Payload.pick(pool)})
    end)

    # ── Run scenarios ───────────────────────────────────────────────────────
    key_range = 10_000
    dur       = cfg.duration_ms()
    conc      = cfg.concurrency()

    scenarios = [
      {"Write-only",              Bench.Scenarios.write_only(pool, key_range)},
      {"Read-local (default)",    Bench.Scenarios.read_only_local(key_range)},
      {"Read-primary",            Bench.Scenarios.read_only_primary(key_range)},
      {"Read-quorum",             Bench.Scenarios.read_only_quorum(key_range)},
      {"Mixed 20% write",         Bench.Scenarios.read_write_mixed(pool, key_range, 20)},
      {"Mixed 50% write",         Bench.Scenarios.read_write_mixed(pool, key_range, 50)},
      {"Mixed + delete",          Bench.Scenarios.delete_mixed(pool, key_range)},
      {"KV write",                Bench.Scenarios.kv_write(key_range)},
      {"KV read",                 Bench.Scenarios.kv_read(key_range)},
      {"Queue enqueue/dequeue",   Bench.Scenarios.queue_throughput()},
      {"Stack push/pop",          Bench.Scenarios.stack_throughput()}
    ]

    results =
      Enum.map(scenarios, fn {label, fun} ->
        IO.write("  running [#{label}]…")
        result = Bench.Worker.run(fun, dur, conc)
        IO.puts(" done")
        Bench.Reporter.print_result(label, result)
        {label, result}
      end)

    Bench.Reporter.print_comparison(results)

    # ── Replication mode comparison (write-only) ───────────────────────────
    if cfg.peers() != [] do
      Bench.Reporter.print_header("Replication Mode Comparison (write-only)")

      mode_results =
        Enum.map([:async, :sync, :strong], fn mode ->
          SuperCache.Config.set_config(:replication_mode, mode)
          Process.sleep(100)
          IO.write("  running [write-only mode=#{mode}]…")
          r = Bench.Worker.run(Bench.Scenarios.write_only(pool, key_range), dur, conc)
          IO.puts(" done")
          Bench.Reporter.print_result("write-only (#{mode})", r)
          {"#{mode}", r}
        end)

      Bench.Reporter.print_comparison(mode_results)

      # Restore original mode
      SuperCache.Config.set_config(:replication_mode, cfg.rep_mode())
    end

    # ── Concurrency scaling test ────────────────────────────────────────────
    Bench.Reporter.print_header("Concurrency Scaling (write-only)")

    conc_results =
      Enum.map([1, 5, 10, 25, 50, 100, 200], fn c ->
        IO.write("  workers=#{c}…")
        r = Bench.Worker.run(Bench.Scenarios.write_only(pool, key_range), dur, c)
        IO.puts(" done")
        Bench.Reporter.print_result("write concurrency=#{c}", r)
        {"conc=#{c}", r}
      end)

    Bench.Reporter.print_comparison(conc_results)

    # ── Payload size scaling ────────────────────────────────────────────────
    Bench.Reporter.print_header("Payload Size Scaling (write-only, concurrency=#{conc})")

    payload_results =
      Enum.map([16, 64, 256, 1_024, 4_096, 16_384], fn bytes ->
        p = Bench.Payload.pool(200, bytes)
        IO.write("  payload=#{bytes}B…")
        r = Bench.Worker.run(Bench.Scenarios.write_only(p, key_range), dur, conc)
        IO.puts(" done")
        Bench.Reporter.print_result("payload=#{bytes}B", r)
        {"#{bytes}B", r}
      end)

    Bench.Reporter.print_comparison(payload_results)

    # ── Final cluster + API stats ───────────────────────────────────────────
    Bench.Reporter.print_header("Post-Benchmark Metrics")
    Bench.Reporter.print_cluster_stats()

    IO.puts("\nDone.\n")

    SuperCache.Cluster.Bootstrap.stop()
  end
end

Bench.Runner.run()
