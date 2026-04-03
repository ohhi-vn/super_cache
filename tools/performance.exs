# =============================================================================
# tools/performance.exs
#
# Comprehensive CLI-driven benchmark suite for SuperCache.
#
# Usage:
#   mix run tools/performance.exs
#   mix run tools/performance.exs --mode write --workers 8 --records 500_000
#   mix run tools/performance.exs --mode all --export-csv results.csv
#   mix run tools/performance.exs --help
#
# Modes: write, read, mixed, lazy_write, kv, queue, stack, memory, all
# =============================================================================

alias :ets, as: Ets

# ── CLI Argument Parsing ──────────────────────────────────────────────────────

defmodule Bench.CLI do
  @moduledoc false

  @defaults %{
    mode: "all",
    workers: 8,
    records: 500_000,
    key_range: 1_000_000,
    warmup: true,
    export_csv: nil,
    partitions: nil,
    table_type: :set,
    help: false
  }

  def parse(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          mode: :string,
          workers: :integer,
          records: :integer,
          key_range: :integer,
          no_warmup: :boolean,
          export_csv: :string,
          partitions: :integer,
          table_type: :string,
          help: :boolean
        ],
        aliases: [
          m: :mode,
          w: :workers,
          n: :records,
          k: :key_range,
          p: :partitions,
          o: :export_csv
        ]
      )

    config =
      @defaults
      |> Map.merge(Enum.into(opts, %{}))
      |> Map.update!(:mode, &String.downcase/1)
      |> Map.update!(:table_type, fn
        "bag" -> :bag
        "duplicate_bag" -> :duplicate_bag
        "ordered_set" -> :ordered_set
        _ -> :set
      end)
      |> Map.put(:warmup, !Keyword.get(opts, :no_warmup, false))

    if config.help do
      print_help()
      System.halt(0)
    end

    validate!(config)
    config
  end

  defp validate!(config) do
    valid_modes = ~w(write read mixed lazy_write kv queue stack memory all)

    unless config.mode in valid_modes do
      IO.puts(
        "Error: Invalid mode '#{config.mode}'. Valid modes: #{Enum.join(valid_modes, ", ")}"
      )

      System.halt(1)
    end

    if config.workers < 1 do
      IO.puts("Error: Workers must be >= 1")
      System.halt(1)
    end

    if config.records < 1 do
      IO.puts("Error: Records must be >= 1")
      System.halt(1)
    end

    config
  end

  defp print_help do
    IO.puts("""
    SuperCache Performance Benchmark

    Usage:
      mix run tools/performance.exs [OPTIONS]

    Options:
      -m, --mode MODE          Benchmark mode (default: all)
                               Modes: write, read, mixed, lazy_write, kv, queue, stack, memory, all
      -w, --workers N          Number of concurrent workers (default: 8)
      -n, --records N          Number of records per worker (default: 500_000)
      -k, --key-range N        Range of keys for random access (default: 1_000_000)
      -p, --partitions N       Number of ETS partitions (default: schedulers_online)
      --table-type TYPE        ETS table type: set, bag, ordered_set (default: set)
      --no-warmup              Disable CPU warmup phase
      -o, --export-csv FILE    Export results to CSV file
      --help                   Show this help message

    Examples:
      # Run all benchmarks with defaults
      mix run tools/performance.exs

      # Run write benchmark with 16 workers
      mix run tools/performance.exs --mode write --workers 16

      # Run memory profiling
      mix run tools/performance.exs --mode memory

      # Export results to CSV
      mix run tools/performance.exs --export-csv results.csv
    """)
  end
end

# ── Statistics Module ─────────────────────────────────────────────────────────

defmodule Bench.Stats do
  @moduledoc false

  def compute(latencies_us) when is_list(latencies_us) do
    count = length(latencies_us)

    if count == 0 do
      %{
        count: 0,
        min_us: 0,
        max_us: 0,
        avg_us: 0,
        p50_us: 0,
        p95_us: 0,
        p99_us: 0,
        ops_per_sec: 0
      }
    else
      sorted = Enum.sort(latencies_us)
      sum = Enum.sum(sorted)

      %{
        count: count,
        min_us: hd(sorted),
        max_us: List.last(sorted),
        avg_us: div(sum, count),
        p50_us: percentile(sorted, count, 50),
        p95_us: percentile(sorted, count, 95),
        p99_us: percentile(sorted, count, 99),
        ops_per_sec: compute_ops_per_sec(count, sum)
      }
    end
  end

  defp percentile(sorted, count, pct) do
    idx = max(0, min(count - 1, trunc(count * pct / 100)))
    Enum.at(sorted, idx)
  end

  defp compute_ops_per_sec(count, total_us) when total_us > 0 do
    round(count / (total_us / 1_000_000))
  end

  defp compute_ops_per_sec(_, _), do: 0

  def format_report(label, stats) do
    """
    ┌─ #{label}
    │  operations=#{format_number(stats.count)}  avg=#{stats.avg_us}µs  p50=#{stats.p50_us}µs  p95=#{stats.p95_us}µs  p99=#{stats.p99_us}µs
    │  throughput=#{format_number(stats.ops_per_sec)} ops/s  min=#{stats.min_us}µs  max=#{stats.max_us}µs
    └─────────────────────────────────────────────────────────────────────────────
    """
  end

  defp format_number(n) when n >= 1_000_000 do
    :io_lib.format("~.1fM", [n / 1_000_000]) |> to_string()
  end

  defp format_number(n) when n >= 1_000 do
    :io_lib.format("~.1fK", [n / 1_000]) |> to_string()
  end

  defp format_number(n), do: to_string(n)
end

# ── Memory Profiler ───────────────────────────────────────────────────────────

defmodule Bench.Memory do
  @moduledoc false

  def profile(label, fun) do
    :erlang.garbage_collect()
    before_mem = :erlang.memory()

    {elapsed_us, result} = :timer.tc(fun)

    :erlang.garbage_collect()
    after_mem = :erlang.memory()

    diff = %{
      total: diff_key(after_mem, before_mem, :total),
      processes: diff_key(after_mem, before_mem, :processes),
      ets: diff_key(after_mem, before_mem, :ets),
      binary: diff_key(after_mem, before_mem, :binary),
      code: diff_key(after_mem, before_mem, :code),
      atom: diff_key(after_mem, before_mem, :atom)
    }

    {result, elapsed_us, diff}
  end

  defp diff_key(after_val, before, key) do
    Map.get(after_val, key, 0) - Map.get(before, key, 0)
  end

  def format_report(label, elapsed_us, mem_diff) do
    """
    ┌─ #{label} (Memory Profile)
    │  duration=#{format_us(elapsed_us)}
    │  memory delta:
    │    total:    #{format_bytes(mem_diff.total)}
    │    ets:      #{format_bytes(mem_diff.ets)}
    │    processes: #{format_bytes(mem_diff.processes)}
    │    binary:   #{format_bytes(mem_diff.binary)}
    │    code:     #{format_bytes(mem_diff.code)}
    │    atom:     #{format_bytes(mem_diff.atom)}
    └─────────────────────────────────────────────────────────────────────────────
    """
  end

  defp format_us(us) when us >= 1_000_000 do
    "#{Float.round(us / 1_000_000, 2)}s"
  end

  defp format_us(us) when us >= 1_000 do
    "#{Float.round(us / 1_000, 2)}ms"
  end

  defp format_us(us), do: "#{us}µs"

  defp format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{Float.round(bytes / 1_073_741_824, 2)} GB"
  end

  defp format_bytes(bytes) when bytes >= 1_048_576 do
    "#{Float.round(bytes / 1_048_576, 2)} MB"
  end

  defp format_bytes(bytes) when bytes >= 1_024 do
    "#{Float.round(bytes / 1_024, 2)} KB"
  end

  defp format_bytes(bytes), do: "#{bytes} B"
end

# ── CSV Exporter ──────────────────────────────────────────────────────────────

defmodule Bench.CSV do
  @moduledoc false

  @headers [
    "benchmark",
    "workers",
    "records",
    "total_ops",
    "avg_us",
    "p50_us",
    "p95_us",
    "p99_us",
    "ops_per_sec",
    "min_us",
    "max_us"
  ]

  def export(results, filepath) do
    csv_content =
      [@headers | Enum.map(results, &to_row/1)]
      |> Enum.map_join("\n", &Enum.join(&1, ","))

    File.write!(filepath, csv_content <> "\n")
    IO.puts("\n✓ Results exported to #{filepath}")
  end

  defp to_row(%{label: label, stats: stats, workers: workers, records: records}) do
    [
      label,
      to_string(workers),
      to_string(records),
      to_string(stats.count),
      to_string(stats.avg_us),
      to_string(stats.p50_us),
      to_string(stats.p95_us),
      to_string(stats.p99_us),
      to_string(stats.ops_per_sec),
      to_string(stats.min_us),
      to_string(stats.max_us)
    ]
  end
end

# ── Benchmark Runner ──────────────────────────────────────────────────────────

defmodule Bench.Runner do
  @moduledoc false

  alias Bench.Stats

  def run_benchmark(label, fun, workers, records_per_worker) do
    # Warmup
    :timer.tc(fn ->
      1..workers
      |> Enum.map(fn _ -> Task.async(fn -> fun.(0, min(1000, records_per_worker)) end) end)
      |> Task.await_many(30_000)
    end)

    # Actual benchmark
    {elapsed_us, latencies} =
      :timer.tc(fn ->
        1..workers
        |> Enum.map(fn _ ->
          Task.async(fn ->
            {task_us, _} = :timer.tc(fn -> fun.(0, records_per_worker) end)
            task_us
          end)
        end)
        |> Task.await_many(300_000)
      end)

    stats = Stats.compute(latencies)
    IO.puts(Stats.format_report(label, stats))

    %{
      label: label,
      stats: stats,
      workers: workers,
      records: records_per_worker * workers,
      elapsed_us: elapsed_us
    }
  end
end

# ── Benchmark Scenarios ───────────────────────────────────────────────────────

defmodule Bench.Scenarios do
  @moduledoc false

  def write_scenario(records_per_worker, key_range) do
    fn start_idx, end_idx ->
      for i <- start_idx..end_idx do
        key = :rand.uniform(key_range)
        SuperCache.put!({key, :value, i})
      end
    end
  end

  def read_scenario(records_per_worker, key_range) do
    # Pre-populate data
    1..records_per_worker
    |> Enum.each(fn i ->
      key = :rand.uniform(key_range)
      SuperCache.put!({key, :value, i})
    end)

    fn start_idx, end_idx ->
      for _ <- start_idx..end_idx do
        key = :rand.uniform(key_range)
        SuperCache.get!({key, nil})
      end
    end
  end

  def mixed_scenario(records_per_worker, key_range, write_pct \\ 20) do
    # Pre-populate some data
    1..div(records_per_worker, 2)
    |> Enum.each(fn i ->
      key = :rand.uniform(key_range)
      SuperCache.put!({key, :value, i})
    end)

    fn start_idx, end_idx ->
      for _ <- start_idx..end_idx do
        key = :rand.uniform(key_range)

        if :rand.uniform(100) <= write_pct do
          SuperCache.put!({key, :value, :rand.uniform(1000)})
        else
          SuperCache.get!({key, nil})
        end
      end
    end
  end

  def lazy_write_scenario(records_per_worker, key_range) do
    fn start_idx, end_idx ->
      for i <- start_idx..end_idx do
        key = :rand.uniform(key_range)
        SuperCache.lazy_put({key, :value, i})
      end
    end
  end

  def kv_scenario(records_per_worker, key_range) do
    alias SuperCache.KeyValue

    fn start_idx, end_idx ->
      for i <- start_idx..end_idx do
        key = "kv_#{:rand.uniform(key_range)}"
        KeyValue.add("bench_kv", key, %{index: i, data: :rand.bytes(64)})
        KeyValue.get("bench_kv", key)
      end
    end
  end

  def queue_scenario(_records_per_worker, _key_range) do
    alias SuperCache.Queue

    fn start_idx, end_idx ->
      for i <- start_idx..end_idx do
        Queue.add("bench_queue", {:item, i})
      end

      for _ <- start_idx..end_idx do
        Queue.out("bench_queue")
      end
    end
  end

  def stack_scenario(_records_per_worker, _key_range) do
    alias SuperCache.Stack

    fn start_idx, end_idx ->
      for i <- start_idx..end_idx do
        Stack.push("bench_stack", {:item, i})
      end

      for _ <- start_idx..end_idx do
        Stack.pop("bench_stack")
      end
    end
  end

  def memory_scenario(records_per_worker, key_range) do
    fn _start, _end ->
      for i <- 1..records_per_worker do
        key = :rand.uniform(key_range)
        SuperCache.put!({key, :value, i, :rand.bytes(128)})
      end
    end
  end
end

# ── Main Execution ────────────────────────────────────────────────────────────

defmodule Bench.Main do
  @moduledoc false

  alias Bench.{CLI, Runner, Scenarios, Stats, Memory, CSV}

  def run do
    config = CLI.parse(System.argv())

    IO.puts("""
    ╔══════════════════════════════════════════════════════════╗
    ║           SuperCache Performance Benchmark              ║
    ╚══════════════════════════════════════════════════════════╝

    Configuration:
      Mode:       #{config.mode}
      Workers:    #{config.workers}
      Records:    #{config.records} per worker
      Key Range:  #{config.key_range}
      Partitions: #{config.partitions || "auto (#{System.schedulers_online()})"}
      Table Type: #{config.table_type}
      Warmup:     #{config.warmup}
    """)

    # Start SuperCache
    opts = [
      key_pos: 0,
      partition_pos: 0,
      table_type: config.table_type
    ]

    opts =
      if config.partitions do
        Keyword.put(opts, :num_partition, config.partitions)
      else
        opts
      end

    SuperCache.start!(opts)

    # Run benchmarks
    results = run_mode(config)

    # Cleanup
    SuperCache.stop()

    # Export if requested
    if config.export_csv do
      CSV.export(results, config.export_csv)
    end

    # Summary
    print_summary(results)
  end

  defp run_mode(%{mode: "write"} = config) do
    [run_single("Write-only", config, &Scenarios.write_scenario/2)]
  end

  defp run_mode(%{mode: "read"} = config) do
    [run_single("Read-only", config, &Scenarios.read_scenario/2)]
  end

  defp run_mode(%{mode: "mixed"} = config) do
    [run_single("Mixed (20% write)", config, &Scenarios.mixed_scenario/3)]
  end

  defp run_mode(%{mode: "lazy_write"} = config) do
    [run_single("Lazy Write", config, &Scenarios.lazy_write_scenario/2)]
  end

  defp run_mode(%{mode: "kv"} = config) do
    [run_single("KeyValue", config, &Scenarios.kv_scenario/2)]
  end

  defp run_mode(%{mode: "queue"} = config) do
    [run_single("Queue", config, &Scenarios.queue_scenario/2)]
  end

  defp run_mode(%{mode: "stack"} = config) do
    [run_single("Stack", config, &Scenarios.stack_scenario/2)]
  end

  defp run_mode(%{mode: "memory"} = config) do
    run_memory_benchmark(config)
  end

  defp run_mode(%{mode: "all"} = config) do
    [
      run_single("Write-only", config, &Scenarios.write_scenario/2),
      run_single("Read-only", config, &Scenarios.read_scenario/2),
      run_single("Mixed (20% write)", config, &Scenarios.mixed_scenario/3),
      run_single("Lazy Write", config, &Scenarios.lazy_write_scenario/2),
      run_single("KeyValue", config, &Scenarios.kv_scenario/2),
      run_single("Queue", config, &Scenarios.queue_scenario/2),
      run_single("Stack", config, &Scenarios.stack_scenario/2)
    ]
  end

  defp run_single(label, config, scenario_fn) do
    IO.puts("\n▶ Running: #{label}...")

    args =
      case Function.info(scenario_fn, :arity) do
        {:arity, 2} -> [config.records, config.key_range]
        {:arity, 3} -> [config.records, config.key_range, 20]
      end

    fun = apply(scenario_fn, args)

    Runner.run_benchmark(
      label,
      fun,
      config.workers,
      config.records
    )
  end

  defp run_memory_benchmark(config) do
    IO.puts("\n▶ Running: Memory Profile...")

    fun = Scenarios.memory_scenario(config.records, config.key_range)

    {_result, elapsed_us, mem_diff} =
      Memory.profile("Memory Profile", fn ->
        fun.(0, config.records)
      end)

    IO.puts(Memory.format_report("Memory Profile", elapsed_us, mem_diff))

    stats = Stats.compute([elapsed_us])

    [
      %{
        label: "Memory Profile",
        stats: stats,
        workers: 1,
        records: config.records,
        elapsed_us: elapsed_us,
        memory_diff: mem_diff
      }
    ]
  end

  defp print_summary(results) do
    if length(results) > 1 do
      IO.puts("\n" <> String.duplicate("═", 70))
      IO.puts("SUMMARY")
      IO.puts(String.duplicate("═", 70))

      results
      |> Enum.sort_by(& &1.stats.ops_per_sec, :desc)
      |> Enum.each(fn r ->
        IO.puts(
          "  #{String.pad_trailing(r.label, 25)} #{String.pad_leading(to_string(r.stats.ops_per_sec), 10)} ops/s"
        )
      end)

      IO.puts(String.duplicate("═", 70))
    end
  end
end

# Execute
Bench.Main.run()
