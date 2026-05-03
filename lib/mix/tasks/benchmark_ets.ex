defmodule Mix.Tasks.BenchmarkEts do
  @moduledoc """
  Run ETS query benchmarks comparing match vs select with compiled specs.

  Usage:
    mix benchmark.ets
  """

  use Mix.Task

  @table :benchmark_table
  @num_records 10_000
  @num_iterations 10_000

  def run(_args) do
    IO.puts("=" |> String.duplicate(70))
    IO.puts("ETS Query Benchmark: match vs select (with compiled match specs)")
    IO.puts("=" |> String.duplicate(70))

    setup_table()
    insert_test_data()
    IO.puts("\nInserted #{@num_records} records into #{inspect(@table)}")
    IO.puts("Running #{@num_iterations} iterations per test...\n")

    # Run benchmarks
    benchmark_match_vs_select()
    benchmark_match_object_vs_select()
    benchmark_tab2list_vs_select_all()

    # Run with compiled specs
    IO.puts("\n" <> "=" |> String.duplicate(70))
    IO.puts("Benchmark: Compiled Match Specs (reused)")
    IO.puts("=" |> String.duplicate(70))
    benchmark_with_compiled_specs()

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
    IO.puts("\n" <> "-" |> String.duplicate(70))
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
    IO.puts("\n" <> "-" |> String.duplicate(70))
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

  defp benchmark_with_compiled_specs do
    IO.puts("Reusing compiled match specs for repeated queries...")

    pattern1 = {:key, :"$1", :"$2", 42}
    match_spec1 = [{pattern1, [], [:"$1", :"$2"]}]
    compiled1 = :ets.match_spec_compile(match_spec1)

    pattern2 = {:key, :_, :_, 42}
    match_spec2 = [{pattern2, [], [:"$_"]}]
    compiled2 = :ets.match_spec_compile(match_spec2)

    match_spec3 = [{:"$_", [], [:"$_"]}]
    compiled3 = :ets.match_spec_compile(match_spec3)

    # Benchmark 1: match vs compiled select
    {time_match, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.match(@table, pattern1)
        end
      end)

    {time_select, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, match_spec1)
        end
      end)

    {time_compiled, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, compiled1)
        end
      end)

    IO.puts("\nTest 1: Find records where 4th element == 42")
    print_results_3ways(":ets.match/2", time_match, ":ets.select/2", time_select, "compiled", time_compiled)

    # Benchmark 2: match_object vs compiled select
    {time_match_obj, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.match_object(@table, pattern2)
        end
      end)

    {time_select2, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, match_spec2)
        end
      end)

    {time_compiled2, _} =
      :timer.tc(fn ->
        for _ <- 1..@num_iterations do
          :ets.select(@table, compiled2)
        end
      end)

    IO.puts("\nTest 2: match_object - find records with 4th element == 42")
    print_results_3ways(":ets.match_object/2", time_match_obj, ":ets.select/2", time_select2, "compiled", time_compiled2)

    # Benchmark 3: tab2list vs compiled select all
    {time_tab2list, _} =
      :timer.tc(fn ->
        for _ <- 1..(@num_iterations |> div(10)) do
          :ets.tab2list(@table)
        end
      end)

    {time_select3, _} =
      :timer.tc(fn ->
        for _ <- 1..(@num_iterations |> div(10)) do
          :ets.select(@table, match_spec3)
        end
      end)

    {time_compiled3, _} =
      :timer.tc(fn ->
        for _ <- 1..(@num_iterations |> div(10)) do
          :ets.select(@table, compiled3)
        end
      end)

    IO.puts("\nTest 3: Get all records")
    print_results_3ways(":ets.tab2list/1", time_tab2list, ":ets.select/2", time_select3, "compiled", time_compiled3)
  end

  defp print_results(before_label, before_time, after_label, after_time) do
    IO.puts("\n#{before_label}: #{format_time(before_time)}")
    IO.puts("#{after_label}: #{format_time(after_time)}")

    improvement = (before_time - after_time) / before_time * 100
    IO.puts("Improvement: #{Float.round(improvement, 2)}% faster with #{after_label}")
  end

  defp print_results_3ways(label1, time1, label2, time2, label3, time3) do
    IO.puts("\n#{label1}: #{format_time(time1)}")
    IO.puts("#{label2}: #{format_time(time2)}")
    IO.puts("#{label3}: #{format_time(time3)}")

    improvement2 = (time1 - time2) / time1 * 100
    improvement3 = (time1 - time3) / time1 * 100

    IO.puts("\nImprovement vs #{label2}: #{Float.round(improvement2, 2)}%")
    IO.puts("Improvement vs #{label3}: #{Float.round(improvement3, 2)}%")
  end

  defp format_time(microseconds) do
    cond do
      microseconds >= 1_000_000 -> "#{Float.round(microseconds / 1_000_000, 2)}s"
      microseconds >= 1_000 -> "#{Float.round(microseconds / 1_000, 2)}ms"
      true -> "#{microseconds}µs"
    end <> " (#{round(microseconds)} µs total)"
  end
end
