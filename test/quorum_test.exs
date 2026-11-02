defmodule SuperCache.Cluster.QuorumTest do
  use ExUnit.Case, async: true

  alias SuperCache.Cluster.Quorum

  describe "await/4" do
    test "returns early when a strict majority agrees" do
      entries = [
        {:a, fn -> :winner end},
        {:b, fn -> :winner end},
        {:c, fn -> Process.sleep(5_000) && :late end}
      ]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 2, 1_000) end)

      assert {:ok, :winner, voters} = result
      assert Enum.sort(voters) == [:a, :b]
      assert messages == []
    end

    test "counts nil as a valid value" do
      entries = [
        {:a, fn -> nil end},
        {:b, fn -> nil end},
        {:c, fn -> :other end}
      ]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 2, 1_000) end)

      assert {:ok, nil, voters} = result
      assert Enum.sort(voters) == [:a, :b]
      assert messages == []
    end

    test "rejects duplicate nodes before starting tasks" do
      entries = [
        {:a, fn -> :value end},
        {:a, fn -> :value end}
      ]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 1, 1_000) end)

      assert {:error, {:duplicate_nodes, [:a]}, %{}} = result
      assert messages == []
    end

    test "returns a timeout with per-node results and kills remaining tasks" do
      entries = [
        {:a, fn -> Process.sleep(5_000) && :a end},
        {:b, fn -> Process.sleep(5_000) && :b end}
      ]

      started = System.monotonic_time(:millisecond)

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 1, 50) end)

      elapsed = System.monotonic_time(:millisecond) - started

      assert {:error, :timeout, %{a: :timeout, b: :timeout}} = result
      assert messages == []
      assert elapsed < 1_000
      assert eventually(fn -> Task.Supervisor.children(Quorum.Pool) == [] end)
    end

    test "a crashing task is a failed vote and cannot crash the caller" do
      entries = [
        {:a, fn -> raise "boom" end},
        {:b, fn -> :ok end},
        {:c, fn -> :ok end}
      ]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 2, 1_000) end)

      assert {:ok, :ok, voters} = result
      assert Enum.sort(voters) == [:b, :c]
      assert messages == []
    end

    test "returns quorum_not_reached when every task crashes" do
      entries = [
        {:a, fn -> exit(:boom) end},
        {:b, fn -> exit(:boom) end}
      ]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 1, 1_000) end)

      assert {:error, :quorum_not_reached, results} = result
      assert map_size(results) == 2
      assert Enum.all?(results, fn {_node, value} -> match?({:error, _}, value) end)
      assert messages == []
    end

    test "returns quorum_not_reached when required exceeds the entry count" do
      entries = [{:a, fn -> :value end}]

      {result, messages} = run_isolated(fn -> Quorum.await(entries, 2, 1_000) end)

      assert {:error, :quorum_not_reached, %{}} = result
      assert messages == []
    end

    test "returns quorum_not_reached for an empty entry list" do
      {result, messages} = run_isolated(fn -> Quorum.await([], 1, 1_000) end)

      assert {:error, :quorum_not_reached, %{}} = result
      assert messages == []
    end
  end

  defp run_isolated(fun) do
    parent = self()

    pid =
      spawn(fn ->
        result = fun.()
        messages = self() |> Process.info(:messages) |> elem(1)
        send(parent, {:quorum_result, result, messages})
      end)

    ref = Process.monitor(pid)

    receive do
      {:quorum_result, result, messages} ->
        Process.demonitor(ref, [:flush])
        {result, messages}

      {:DOWN, ^ref, :process, _pid, reason} ->
        flunk("quorum caller crashed: #{inspect(reason)}")
    after
      5_000 ->
        flunk("quorum caller did not return within the test timeout")
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
