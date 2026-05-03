defmodule CompiledBench do
  @table :compiled_bench
  @num_records 10_000
  @num_iterations 100_000

  def run do
    IO.puts("=" |> String.duplicate(70))
    IO.puts("Benchmark: Compiled Match Spec Benefits")
    IO.puts("=" |> String.duplicate(70))

    :ets.new(@table, [:set, :named_table, :public])
    
    for i <- 1..@num_records do
      :ets.insert(@table, {:key, i, "value_#{i}", rem(i, 100)})
    end

    IO.puts("\nInserted #{@num_records} records")
    IO.puts("Running #{@num_iterations} iterations...\n")

    # Test 1: Simple pattern
    benchmark_simple()

    # Test 2: Complex pattern with guards
    benchmark_with_guards()

    :ets.delete(@table)
  end

  defp benchmark_simple do
    IO.puts("-" |> String.duplicate(70))
    IO.puts("Test 1: Simple pattern (4th element == 42)")
    IO.puts("-" |> String.duplicate(70))

    pattern = {:key, :"$1", :"$2", 42}
    match_spec = [{pattern, [], [:"$1", :"$2"]}]
    
    # Without compiled spec
    {time1, _} = :timer.tc(fn ->
      for _ <- 1..@num_iterations do
        :ets.match(@table, pattern)
      end
    end)

    # With match_spec (not compiled)
    {time2, _} = :timer.tc(fn ->
      for _ <- 1..@num_iterations do
        :ets.select(@table, match_spec)
      end
    end)

    # With compiled spec (compile once, reuse)
    compiled = :ets.match_spec_compile(match_spec)
    {time3, _} = :timer.tc(fn ->
      for _ <- 1..@num_iterations do
        :ets.select(@table, compiled)
      end
    end)

    print_results_3way("match/2", time1, "select/2", time2, "compiled", time3)
  end

  defp benchmark_with_guards do
    IO.puts("\n" <> "-" |> String.duplicate(70))
    IO.puts("Test 2: Complex pattern WITH GUARDS (where compiled specs help)")
    IO.puts("-" |> String.duplicate(70))

    # Pattern with guard: 4th element > 50
    match_spec = [{:_, :_, :_, :"$1"}, [{:>, :"$1", 50}], [:"$1"]]
    
    # Using select with match spec directly
    {time1, _} = :timer.tc(fn ->
      for _ <- 1..(@num_iterations |> div(10)) do
        :ets.select(@table, match_spec)
      end
    end)

    # Using compiled spec
    compiled = :ets.match_spec_compile(match_spec)
    {time2, _} = :timer.tc(fn ->
      for _ <- 1..(@num_iterations |> div(10)) do
        :ets.select(@table, compiled)
      end
    end)

    IO.puts("\nselect/2 (with guard): #{format_time(time1)}")
    IO.puts("compiled spec (with guard): #{format_time(time2)}")
    
    improvement = (time1 - time2) / time1 * 100
    IO.puts("Improvement: #{Float.round(improvement, 2)}% faster with compiled spec")
  end

  defp print_results_3way(label1, time1, label2, time2, label3, time3) do
    IO.puts("\n#{label1}: #{format_time(time1)}")
    IO.puts("#{label2}: #{format_time(time2)}")
    IO.puts("#{label3}: #{format_time(time3)}")
    
    imp2 = (time1 - time2) / time1 * 100
    imp3 = (time1 - time3) / time1 * 100
    
    IO.puts("\nImprovement #{label2}: #{Float.round(imp2, 2)}%")
    IO.puts("Improvement #{label3}: #{Float.round(imp3, 2)}%")
  end

  defp format_time(us) do
    cond do
      us >= 1_000_000 -> "#{Float.round(us / 1_000_000, 2)}s"
      us >= 1_000 -> "#{Float.round(us / 1_000, 2)}ms"
      true -> "#{us}µs"
    end <> " (#{round(us)} µs total)"
  end
end

CompiledBench.run()
