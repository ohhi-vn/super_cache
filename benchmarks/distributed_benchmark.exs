defmodule SuperCache.DistributedBenchmark do
  @moduledoc """
  End-to-end benchmark for distributed write and read modes.

  Run with a distributed node name so `:peer` can start:

      elixir --name bench@127.0.0.1 --cookie bench_secret \\
        -S mix run benchmarks/distributed_benchmark.exs

  By default the script builds a 3-node cluster (this node plus 2 peers).
  Set `BENCH_NODES` to change the cluster size, e.g. for a 1-node or 5-node
  run:

      BENCH_NODES=1 elixir --name bench@127.0.0.1 --cookie bench_secret \\
        -S mix run benchmarks/distributed_benchmark.exs

      BENCH_NODES=5 elixir --name bench@127.0.0.1 --cookie bench_secret \\
        -S mix run benchmarks/distributed_benchmark.exs

  The script starts an N-node cluster for each replication mode and
  reports throughput and per-operation latency for puts, primary reads, and
  quorum reads.

  Set `BENCH_RF` to change `replication_factor` (default 2), e.g.
  `BENCH_RF=3 BENCH_NODES=5 ...`. A factor of 3 gives Raft a three-voter
  group, which is the recommended production minimum.
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

  defp total_nodes do
    case System.get_env("BENCH_NODES") do
      nil -> 3
      v -> String.to_integer(v)
    end
  end

  defp replication_factor do
    case System.get_env("BENCH_RF") do
      nil -> 2
      v -> String.to_integer(v)
    end
  end

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

    Enum.each([:async, :sync, :strong, :raft], &run_mode/1)
  end

  defp run_mode(mode) do
    IO.puts("\n" <> String.duplicate("=", 68))
    IO.puts("distributed benchmark — replication_mode: #{mode}")
    IO.puts(String.duplicate("=", 68))

    n = total_nodes()

    raft_dir =
      if mode == :raft do
        d =
          Path.join(
            System.tmp_dir!(),
            "sc_bench_raft_#{mode}_#{System.unique_integer([:positive])}"
          )

        File.mkdir_p!(d)
        d
      else
        nil
      end

    stop_cluster()
    Process.sleep(200)

    base_opts =
      if mode == :raft do
        Keyword.put(@opts, :num_partition, 1)
      else
        @opts
      end

    opts =
      base_opts
      |> Keyword.put(:replication_mode, mode)
      |> Keyword.put(:replication_factor, replication_factor())

    if mode == :raft do
      local_dir = Path.join(raft_dir, "local")
      File.mkdir_p!(local_dir)
      Application.put_env(:super_cache, :raft, data_dir: local_dir)
      SuperCache.Cluster.Raft.Config.reset()
    end

    :ok = SuperCache.Cluster.Bootstrap.start!(opts)

    peers =
      for i <- 1..(n - 1)//1 do
        start_peer(String.to_atom("bench_#{mode}_peer#{i}"), opts, raft_dir, i)
      end

    peer_nodes = Enum.map(peers, fn {_, node} -> node end)

    Enum.each(peer_nodes, fn nd -> Node.connect(nd) end)

    for i <- 0..(length(peer_nodes) - 1)//1 do
      for j <- (i + 1)..(length(peer_nodes) - 1)//1 do
        a = Enum.at(peer_nodes, i)
        b = Enum.at(peer_nodes, j)
        :erpc.call(a, Node, :connect, [b], 10_000)
      end
    end

    if peer_nodes != [] do
      SuperCache.Cluster.NodeMonitor.reconfigure(nodes: peer_nodes)

      Enum.each(peer_nodes, fn nd ->
        others = [node()] ++ (peer_nodes -- [nd])
        :erpc.call(nd, SuperCache.Cluster.NodeMonitor, :reconfigure, [[nodes: others]])
      end)
    end

    wait_until(fn -> length(SuperCache.Cluster.Manager.live_nodes()) == n end, 90_000)

    if mode == :raft do
      # Ensure raft groups are reconciled on every partition, one at a time.
      num_partitions = Keyword.get(opts, :num_partition, 2)

      Enum.each(0..(num_partitions - 1), fn idx ->
        {primary, _} = SuperCache.Cluster.Manager.get_replicas(idx)

        try do
          :erpc.call(primary, SuperCache.Cluster.Raft.Group.Reconciler, :reconcile, [idx], 60_000)
        catch
          _, _ -> :ok
        end

        Process.sleep(200)
      end)

      # Wait for leaders to be elected.
      wait_until(
        fn ->
          Enum.all?(0..(num_partitions - 1), fn idx ->
            match?({:ok, _}, RaftEx.find_leader(SuperCache.Cluster.Raft.Group.cluster_name(idx)))
          end)
        end,
        60_000
      )
    end

    Process.sleep(500)

    Enum.each(1..@warmup, fn i ->
      true = SuperCache.put!({:bench, mode, i, "warmup"})
    end)

    put_us =
      time(fn ->
        Enum.each(1..@put_ops, fn i ->
          true = SuperCache.put!({:bench, mode, i, "value-#{i}"})
        end)
      end)

    primary_read_us =
      time(fn ->
        Enum.each(1..@read_ops, fn i ->
          [_] = SuperCache.get!({:bench, mode, rem(i, @put_ops) + 1, nil}, read_mode: :primary)
        end)
      end)

    quorum_read_us =
      time(fn ->
        Enum.each(1..@read_ops, fn i ->
          [_] = SuperCache.get!({:bench, mode, rem(i, @put_ops) + 1, nil}, read_mode: :quorum)
        end)
      end)

    print_result("put!", @put_ops, put_us)
    print_result("get! :primary", @read_ops, primary_read_us)
    print_result("get! :quorum", @read_ops, quorum_read_us)

    Enum.each(peers, fn {peer, node} ->
      stop_peer(peer)
      Node.disconnect(node)
    end)

    stop_cluster()

    # Wait for all peer connections to drop so the next mode starts clean.
    wait_until(fn -> Node.list() == [] end, 10_000)

    if mode == :raft do
      try do
        SuperCache.Cluster.Raft.System.stop()
      catch
        :exit, _ -> :ok
      end

      if raft_dir, do: File.rm_rf!(raft_dir)
    end

    Process.sleep(300)
  end

  defp start_peer(name, opts, raft_dir \\ nil, idx \\ 1) do
    cookie = :erlang.get_cookie()
    code_path = :code.get_path()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie",
          :erlang.atom_to_list(cookie),
          ~c"-connect_all",
          ~c"false"
        ]
      })

    Process.sleep(1_000)
    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])

    if raft_dir do
      peer_data_dir = Path.join(raft_dir, "peer#{idx}")
      File.mkdir_p!(peer_data_dir)
      :erpc.call(node, Application, :put_env, [:super_cache, :raft, [data_dir: peer_data_dir]])
      :erpc.call(node, SuperCache.Cluster.Raft.Config, :reset, [])
    end

    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [opts], 280_000)

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

    IO.puts(
      "#{label}: #{ops} ops in #{us}µs, #{Float.round(per_op_us, 2)}µs/op, #{format_rate(ops_per_second)} ops/s"
    )
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
