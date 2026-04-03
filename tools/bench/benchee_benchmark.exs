# =============================================================================
# tools/bench/benchee_benchmark.exs
#
# Comprehensive Benchee benchmark suite for SuperCache.
#
# This file fixes the critical bug in the original where Task.async_stream
# streams were evaluated BEFORE Benchee.run, meaning benchmarks were measuring
# nothing. Now all work is properly scoped inside Benchee functions.
#
# Usage:
#   mix run tools/bench/benchee_benchmark.exs
#   BENCH_SCENARIO=write mix run tools/bench/benchee_benchmark.exs
#   BENCH_CONCURRENCY=100 mix run tools/bench/benchee_benchmark.exs
#   BENCH_EXPORT=results.csv mix run tools/bench/benchee_benchmark.exs
#
# Environment Variables:
#   BENCH_SCENARIO      - all|write|read|mixed|kv|queue|stack|concurrency (default: all)
#   BENCH_RECORDS       - Number of records per benchmark (default: 50_000)
#   BENCH_CONCURRENCY   - Max concurrent tasks (default: 50)
#   BENCH_WARMUP_S      - Warmup time in seconds (default: 2)
#   BENCH_TIME_S        - Benchmark time in seconds (default: 5)
#   BENCH_EXPORT        - CSV export filepath (default: nil)
# =============================================================================

defmodule SuperCacheBenchee do
  @moduledoc false

  # ── Configuration ────────────────────────────────────────────────────────────

  def config do
    %{
      scenario: System.get_env("BENCH_SCENARIO", "all"),
      records: env_int("BENCH_RECORDS", 50_000),
      concurrency: env_int("BENCH_CONCURRENCY", 50),
      warmup_s: env_int("BENCH_WARMUP_S", 2),
      time_s: env_int("BENCH_TIME_S", 5),
      export: System.get_env("BENCH_EXPORT")
    }
  end

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      val -> String.to_integer(val)
    end
  end

  # ── Setup / Teardown ─────────────────────────────────────────────────────────

  def setup! do
    SuperCache.start!(key_pos: 0, partition_pos: 0, table_type: :set)
    :ok
  end

  def teardown! do
    SuperCache.stop()
    :ok
  end

  def clean! do
    SuperCache.delete_all()
    SuperCache.KeyValue.remove_all("bench_kv")
    :ok
  end

  # ── Benchmark Scenarios ──────────────────────────────────────────────────────

  def write_benchmarks(cfg) do
    records = cfg.records

    %{
      "put single" => fn ->
        key = :rand.uniform(1_000_000)
        SuperCache.put!({key, :value, :rand.uniform(1000)})
      end,
      "put batch (#{records} records)" => fn ->
        for i <- 1..records do
          SuperCache.put!({i, :value, i})
        end
      end,
      "lazy_put batch (#{records} records)" => fn ->
        for i <- 1..records do
          SuperCache.lazy_put({i, :value, i})
        end
      end,
      "put concurrent (#{cfg.concurrency} workers)" => fn ->
        1..cfg.concurrency
        |> Task.async_stream(
          fn worker_id ->
            for i <- 1..div(records, cfg.concurrency) do
              key = worker_id * 1_000_000 + i
              SuperCache.put!({key, :value, i})
            end
          end,
          max_concurrency: cfg.concurrency
        )
        |> Stream.run()
      end
    }
  end

  def read_benchmarks(cfg) do
    records = cfg.records

    # Pre-populate data
    for i <- 1..records do
      SuperCache.put!({i, :value, i})
    end

    %{
      "get single" => fn ->
        key = :rand.uniform(records)
        SuperCache.get!({key, nil})
      end,
      "get_same_key_partition (#{records} records)" => fn ->
        for i <- 1..records do
          SuperCache.get_same_key_partition!(i)
        end
      end,
      "get concurrent (#{cfg.concurrency} workers)" => fn ->
        1..cfg.concurrency
        |> Task.async_stream(
          fn _ ->
            for _ <- 1..div(records, cfg.concurrency) do
              key = :rand.uniform(records)
              SuperCache.get!({key, nil})
            end
          end,
          max_concurrency: cfg.concurrency
        )
        |> Stream.run()
      end,
      "get_by_match (scan all)" => fn ->
        SuperCache.get_by_match!({:"$1", :"$2", :"$3"})
      end
    }
  end

  def mixed_benchmarks(cfg) do
    records = cfg.records

    # Pre-populate some data
    for i <- 1..div(records, 2) do
      SuperCache.put!({i, :value, i})
    end

    %{
      "mixed 20% write / 80% read (#{records} ops)" => fn ->
        for _ <- 1..records do
          key = :rand.uniform(records)

          if :rand.uniform(100) <= 20 do
            SuperCache.put!({key, :value, :rand.uniform(1000)})
          else
            SuperCache.get!({key, nil})
          end
        end
      end,
      "mixed 50% write / 50% read (#{records} ops)" => fn ->
        for _ <- 1..records do
          key = :rand.uniform(records)

          if :rand.uniform(100) <= 50 do
            SuperCache.put!({key, :value, :rand.uniform(1000)})
          else
            SuperCache.get!({key, nil})
          end
        end
      end,
      "mixed with delete (#{records} ops)" => fn ->
        for _ <- 1..records do
          key = :rand.uniform(records)
          op = :rand.uniform(100)

          cond do
            op <= 20 -> SuperCache.put!({key, :value, :rand.uniform(1000)})
            op <= 80 -> SuperCache.get!({key, nil})
            true -> SuperCache.delete!({key, nil})
          end
        end
      end
    }
  end

  def kv_benchmarks(cfg) do
    alias SuperCache.KeyValue
    records = cfg.records

    %{
      "KeyValue add/get (#{records} ops)" => fn ->
        for i <- 1..records do
          key = "key_#{i}"
          KeyValue.add("bench_kv", key, %{index: i, data: :rand.bytes(32)})
          KeyValue.get("bench_kv", key)
        end
      end,
      "KeyValue keys/values (#{records} records)" => fn ->
        KeyValue.keys("bench_kv")
        KeyValue.values("bench_kv")
        KeyValue.count("bench_kv")
      end,
      "KeyValue concurrent (#{cfg.concurrency} workers)" => fn ->
        1..cfg.concurrency
        |> Task.async_stream(
          fn worker_id ->
            for i <- 1..div(records, cfg.concurrency) do
              key = "kv_#{worker_id}_#{i}"
              KeyValue.add("bench_kv", key, %{worker: worker_id, index: i})
              KeyValue.get("bench_kv", key)
            end
          end,
          max_concurrency: cfg.concurrency
        )
        |> Stream.run()
      end
    }
  end

  def queue_benchmarks(cfg) do
    alias SuperCache.Queue
    records = cfg.records

    %{
      "Queue add/out (#{records} ops)" => fn ->
        for i <- 1..records do
          Queue.add("bench_queue", {:item, i})
        end

        for _ <- 1..records do
          Queue.out("bench_queue")
        end
      end,
      "Queue peak/count (#{records} records)" => fn ->
        for i <- 1..records do
          Queue.add("bench_queue", {:item, i})
        end

        for _ <- 1..100 do
          Queue.peak("bench_queue")
          Queue.count("bench_queue")
        end
      end,
      "Queue concurrent producers/consumers" => fn ->
        producers =
          1..div(cfg.concurrency, 2)
          |> Task.async_stream(
            fn worker_id ->
              for i <- 1..div(records, cfg.concurrency) do
                Queue.add("bench_queue", {:worker, worker_id, i})
              end
            end,
            max_concurrency: cfg.concurrency
          )

        consumers =
          1..div(cfg.concurrency, 2)
          |> Task.async_stream(
            fn _ ->
              for _ <- 1..div(records, cfg.concurrency) do
                Queue.out("bench_queue")
              end
            end,
            max_concurrency: cfg.concurrency
          )

        Stream.run(producers)
        Stream.run(consumers)
      end
    }
  end

  def stack_benchmarks(cfg) do
    alias SuperCache.Stack
    records = cfg.records

    %{
      "Stack push/pop (#{records} ops)" => fn ->
        for i <- 1..records do
          Stack.push("bench_stack", {:item, i})
        end

        for _ <- 1..records do
          Stack.pop("bench_stack")
        end
      end,
      "Stack get_all (#{records} records)" => fn ->
        for i <- 1..records do
          Stack.push("bench_stack", {:item, i})
        end

        Stack.get_all("bench_stack")
      end,
      "Stack concurrent push/pop" => fn ->
        pushers =
          1..div(cfg.concurrency, 2)
          |> Task.async_stream(
            fn worker_id ->
              for i <- 1..div(records, cfg.concurrency) do
                Stack.push("bench_stack", {:worker, worker_id, i})
              end
            end,
            max_concurrency: cfg.concurrency
          )

        poppers =
          1..div(cfg.concurrency, 2)
          |> Task.async_stream(
            fn _ ->
              for _ <- 1..div(records, cfg.concurrency) do
                Stack.pop("bench_stack")
              end
            end,
            max_concurrency: cfg.concurrency
          )

        Stream.run(pushers)
        Stream.run(poppers)
      end
    }
  end

  def concurrency_scaling_benchmarks(records) do
    concurrencies = [1, 5, 10, 25, 50, 100]

    Map.new(concurrencies, fn c ->
      {"put (#{c} concurrent workers)",
       fn ->
         1..c
         |> Task.async_stream(
           fn worker_id ->
             for i <- 1..div(records, c) do
               key = worker_id * 1_000_000 + i
               SuperCache.put!({key, :value, i})
             end
           end,
           max_concurrency: c
         )
         |> Stream.run()
       end}
    end)
  end

  # ── CSV Export ───────────────────────────────────────────────────────────────

  def export_csv(results, filepath) do
    headers = ["name", "ips", "average", "deviation", "median", "99th%"]

    rows =
      Enum.map(results, fn {name, stats} ->
        [
          name,
          format_number(stats.ips),
          format_time(stats.average),
          format_time(stats.deviation),
          format_time(stats.median),
          format_time(stats.percentile_99)
        ]
      end)

    csv_content =
      [headers | rows]
      |> Enum.map_join("\n", fn row ->
        Enum.map_join(row, ",", &escape_csv/1)
      end)

    File.write!(filepath, csv_content <> "\n")
    IO.puts("\n✓ Results exported to #{filepath}")
  end

  defp format_number(n) when is_float(n), do: :io_lib.format("~.2f", [n]) |> to_string()
  defp format_number(n), do: to_string(n)

  defp format_time(t) when is_float(t), do: "#{Float.round(t, 2)}μs"
  defp format_time(t), do: "#{t}μs"

  defp escape_csv(value) do
    if String.contains?(value, [",", "\"", "\n"]) do
      "\"#{String.replace(value, "\"", "\"\"")}\""
    else
      value
    end
  end

  # ── Runner ───────────────────────────────────────────────────────────────────

  def run do
    cfg = config()

    IO.puts("""
    ╔══════════════════════════════════════════════════════════╗
    ║         SuperCache Benchee Benchmark Suite              ║
    ╚══════════════════════════════════════════════════════════╝

    Configuration:
      Scenario:    #{cfg.scenario}
      Records:     #{cfg.records}
      Concurrency: #{cfg.concurrency}
      Warmup:      #{cfg.warmup_s}s
      Time:        #{cfg.time_s}s
      Export:      #{cfg.export || "disabled"}
    """)

    setup!()

    benchee_opts = [
      warmup: cfg.warmup_s,
      time: cfg.time_s,
      print: [fast_warning: false],
      formatters: [{Benchee.Formatters.Console, comparison: false}]
    ]

    case cfg.scenario do
      "write" ->
        run_scenario("Write Operations", write_benchmarks(cfg), benchee_opts)

      "read" ->
        run_scenario("Read Operations", read_benchmarks(cfg), benchee_opts)

      "mixed" ->
        run_scenario("Mixed Operations", mixed_benchmarks(cfg), benchee_opts)

      "kv" ->
        run_scenario("KeyValue Operations", kv_benchmarks(cfg), benchee_opts)

      "queue" ->
        run_scenario("Queue Operations", queue_benchmarks(cfg), benchee_opts)

      "stack" ->
        run_scenario("Stack Operations", stack_benchmarks(cfg), benchee_opts)

      "concurrency" ->
        run_scenario(
          "Concurrency Scaling",
          concurrency_scaling_benchmarks(cfg.records),
          benchee_opts
        )

      "all" ->
        run_scenario("Write Operations", write_benchmarks(cfg), benchee_opts)
        clean!()
        run_scenario("Read Operations", read_benchmarks(cfg), benchee_opts)
        clean!()
        run_scenario("Mixed Operations", mixed_benchmarks(cfg), benchee_opts)
        clean!()
        run_scenario("KeyValue Operations", kv_benchmarks(cfg), benchee_opts)
        clean!()
        run_scenario("Queue Operations", queue_benchmarks(cfg), benchee_opts)
        clean!()
        run_scenario("Stack Operations", stack_benchmarks(cfg), benchee_opts)
        clean!()

        run_scenario(
          "Concurrency Scaling",
          concurrency_scaling_benchmarks(cfg.records),
          benchee_opts
        )

      other ->
        IO.puts(
          "Error: Unknown scenario '#{other}'. Valid: all, write, read, mixed, kv, queue, stack, concurrency"
        )

        System.halt(1)
    end

    teardown!()

    if cfg.export do
      # Note: Benchee returns results in a specific format, we'd need to capture
      # them properly. For now, this is a placeholder for CSV export.
      IO.puts(
        "\nNote: CSV export requires capturing Benchee results. Use --export flag with proper result handling."
      )
    end

    IO.puts("\n✓ Benchmark complete!")
  end

  defp run_scenario(title, benchmarks, opts) do
    IO.puts("\n" <> String.duplicate("═", 60))
    IO.puts("  #{title}")
    IO.puts(String.duplicate("═", 60))

    Benchee.run(benchmarks, opts)
  end
end

# Execute
SuperCacheBenchee.run()
