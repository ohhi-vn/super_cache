defmodule SuperCache.DistributedBenchmark do
  @moduledoc """
  End-to-end benchmark for distributed write and read modes.

  Run with a distributed node name so `:peer` can start:

      elixir --name bench@127.0.0.1 --cookie bench_secret \\
        -S mix run benchmarks/distributed_benchmark.exs

  The script starts a three-node cluster for each replication mode and
  reports throughput and per-operation latency for puts, primary reads, and
  quorum reads.
  """

  @opts [
    key_pos: 0,
    partition_pos: 0,
    cluster: :distributed,
    replication_factor: 2,
    num_partition: 8,
    table_type: :set
  ]

  @warmup 100
  @put_ops 500
  @read_ops 1_000

  def run do
    if node() == :nonode@nohost do
      IO.puts("""
      distributed benchmark requires a distributed node; run:

        elixir --name bench@127.0.0.1 --cookie bench_secret \\
          -S mix run benchmarks/distributed_benchmark.exs
      """)

      System.halt(1)
    end

    {:ok, _} = Application.ensure_all_started(:super_cache)

    Enum.each([:async, :sync, :strong], &run_mode/1)
  end

  defp run_mode(mode) do
    IO.puts("\n" <> String.duplicate("=", 68))
    IO.puts("distributed benchmark — replication_mode: #{mode}")
    IO.puts(String.duplicate("=", 68))

    stop_cluster()
    Process.sleep(200)

    opts = Keyword.put(@opts, :replication_mode, mode)
    :ok = SuperCache.Cluster.Bootstrap.start!(opts)

    {peer1, node1} = start_peer(:"bench_#{mode}_peer1", opts)
    {peer2, node2} = start_peer(:"bench_#{mode}_peer2", opts)

    true = Node.connect(node1)
    true = Node.connect(node2)
    true = :erpc.call(node1, Node, :connect, [node2], 10_000)

    wait_until(fn -> length(SuperCache.Cluster.Manager.live_nodes()) == 3 end, 15_000)

    SuperCache.Cluster.NodeMonitor.reconfigure(nodes: [node1, node2])
    :erpc.call(node1, SuperCache.Cluster.NodeMonitor, :reconfigure, [[nodes: [node(), node2]]])
    :erpc.call(node2, SuperCache.Cluster.NodeMonitor, :reconfigure, [[nodes: [node(), node1]]])

    wait_until(fn -> length(SuperCache.Cluster.Manager.live_nodes()) == 3 end, 15_000)
    Process.sleep(300)

    Enum.each(1..@warmup, fn i ->
      true = SuperCache.put!({:bench, mode, i, "warmup"})
    end)

    put_us = time(fn ->
      Enum.each(1..@put_ops, fn i ->
        true = SuperCache.put!({:bench, mode, i, "value-#{i}"})
      end)
    end)

    primary_read_us = time(fn ->
      Enum.each(1..@read_ops, fn i ->
        [_] = SuperCache.get!({:bench, mode, rem(i, @put_ops) + 1, nil}, read_mode: :primary)
      end)
    end)

    quorum_read_us = time(fn ->
      Enum.each(1..@read_ops, fn i ->
        [_] = SuperCache.get!({:bench, mode, rem(i, @put_ops) + 1, nil}, read_mode: :quorum)
      end)
    end)

    print_result("put!", @put_ops, put_us)
    print_result("get! :primary", @read_ops, primary_read_us)
    print_result("get! :quorum", @read_ops, quorum_read_us)

    stop_peer(peer1)
    stop_peer(peer2)
    stop_cluster()
    Process.sleep(300)
  end

  defp start_peer(name, opts) do
    cookie = :erlang.get_cookie()
    code_path = :code.get_path()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie", :erlang.atom_to_list(cookie),
          ~c"-connect_all", ~c"false"
        ]
      })

    Process.sleep(1_000)
    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [opts], 30_000)

    {peer, node}
  end

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp stop_cluster do
    try do
      SuperCache.Cluster.Bootstrap.stop()
    catch
      :exit, _ -> :ok
    end
  end

  defp time(fun) do
    {us, _} = :timer.tc(fun)
    us
  end

  defp print_result(label, ops, us) do
    per_op_us = us / ops
    ops_per_second = if us == 0, do: :infinity, else: ops * 1_000_000 / us
    IO.puts("#{label}: #{ops} ops in #{us}µs, #{Float.round(per_op_us, 2)}µs/op, #{format_rate(ops_per_second)} ops/s")
  end

  defp format_rate(:infinity), do: "infinity"

  defp format_rate(rate) do
    rate
    |> round()
    |> Integer.to_string()
    |> String.replace(~r/(\d)(?=(\d{3})+(?!\d))/, "\\1,")
  end

  defp wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "benchmark cluster did not converge before the deadline"

      true ->
        Process.sleep(50)
        do_wait_until(fun, deadline)
    end
  end
end

SuperCache.DistributedBenchmark.run()
