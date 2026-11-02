defmodule Mix.Tasks.BenchmarkEts do
  @moduledoc """
  Run ETS query benchmarks comparing match vs select.

  Usage:
    mix benchmark.ets
  """

  use Mix.Task

  @table :benchmark_table
  @num_records 10_000
  @num_iterations 10_000

  def run(_args) do
    IO.puts("=" |> String.duplicate(70))
    IO.puts("ETS Query Benchmark: match vs select")
    IO.puts("=" |> String.duplicate(70))

    setup_table()
    insert_test_data()
    IO.puts("\nInserted #{@num_records} records into #{inspect(@table)}")
    IO.puts("Running #{@num_iterations} iterations per test...\n")

    # Run benchmarks
    benchmark_match_vs_select()
    benchmark_match_object_vs_select()
    benchmark_tab2list_vs_select_all()

    cleanup_table()
  end

  defp setup_table do
    try do
      :ets.delete(@table)
    rescue
      _ -> :ok
    end

    :ets.new(@table, [:set, :named_table, :public])
  end

  defp cleanup_table do
    :ets.delete(@table)
  end

  defp insert_test_data do
    for i <- 1..@num_records do
      :ets.insert(@table, {:key, i, "value_#{i}", rem(i, 100)})
    end
  end

  defp benchmark_match_vs_select do
    IO.puts("-" |> String.duplicate(70))
    IO.puts("Test 1: Find records where 4th element == 42")
    IO.puts("-" |> String.duplicate(70))

    pattern = {:key, :"$1", :"$2", 42}
    match_spec = [{pattern, [], [:"$1", :"$2"]}]

    # Before: Using :ets.match/2
    {time_before, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.match(@table, pattern)
        end
      end)

    # After: Using :ets.select/2 with match spec
    {time_after, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, match_spec)
        end
      end)

    print_results(":ets.match/2", time_before, ":ets.select/2", time_after)
  end

  defp benchmark_match_object_vs_select do
    IO.puts(("\n" <> "-") |> String.duplicate(70))
    IO.puts("Test 2: match_object - find records with 4th element == 42")
    IO.puts("-" |> String.duplicate(70))

    pattern = {:key, :_, :_, 42}
    match_spec = [{pattern, [], [:"$_"]}]

    # Before: Using :ets.match_object/2
    {time_before, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.match_object(@table, pattern)
        end
      end)

    # After: Using :ets.select/2 with match spec
    {time_after, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, match_spec)
        end
      end)

    print_results(":ets.match_object/2", time_before, ":ets.select/2", time_after)
  end

  defp benchmark_tab2list_vs_select_all do
    IO.puts(("\n" <> "-") |> String.duplicate(70))
    IO.puts("Test 3: Get all records (tab2list vs select)")
    IO.puts("-" |> String.duplicate(70))

    match_spec = [{:"$_", [], [:"$_"]}]

    # Before: Using :ets.tab2list/1
    {time_before, _} =
      :timer.tc(fn ->
        for _ <- 1..(@num_iterations |> div(10)) do
          :ets.tab2list(@table)
        end
      end)

    # After: Using :ets.select/2 with match spec
    {time_after, _} =
      :timer.tc(fn ->
        for _ <- 1..(@num_iterations |> div(10)) do
          :ets.select(@table, match_spec)
        end
      end)

    print_results(":ets.tab2list/1", time_before, ":ets.select/2", time_after)
  end

  defp print_results(before_label, before_time, after_label, after_time) do
    IO.puts("\n#{before_label}: #{format_time(before_time)}")
    IO.puts("#{after_label}: #{format_time(after_time)}")

    improvement = (before_time - after_time) / before_time * 100
    IO.puts("Improvement: #{Float.round(improvement, 2)}% faster with #{after_label}")
  end

  defp format_time(microseconds) do
    cond do
      microseconds >= 1_000_000 -> "#{Float.round(microseconds / 1_000_000, 2)}s"
      microseconds >= 1_000 -> "#{Float.round(microseconds / 1_000, 2)}ms"
      true -> "#{microseconds}µs"
    end <> " (#{round(microseconds)} µs total)"
  end
end
