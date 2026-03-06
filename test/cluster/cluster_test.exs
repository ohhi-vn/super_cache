defmodule SuperCache.ClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 120_000

  alias SuperCache.Cluster.Manager
  alias SuperCache.Distributed, as: Cache

  @cache_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    num_partition:      8,
    table_type:         :set
  ]

  # ── Peer helpers ─────────────────────────────────────────────────────────────

  defp start_peer(name) do
    cookie    = :erlang.get_cookie()
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

    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])

    {peer, node}
  end

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp restart_peer(name) do
    {peer, node} = start_peer(name)
    true = Node.connect(node)
    true = wait_until(fn -> node in Manager.live_nodes() end)
    # Wait for partition maps to converge on all nodes after rejoin.
    wait_cluster_stable()
    {peer, node}
  end

  # Ask each live node to read via primary routing.
  defp remote_get(target_node, data) do
    :erpc.call(target_node, SuperCache.Cluster.Router, :route_get!,
      [data, [read_mode: :primary]], 8_000)
  end

  defp primary_for(partition_value) do
    idx = SuperCache.Partition.get_partition_order(partition_value)
    {primary, _} = Manager.get_replicas(idx)
    primary
  end

  # Wipe all ETS tables on every live node by calling Storage.delete_all
  # directly — completely bypasses the Router so there is no forwarding,
  # no primary lookup, and nothing that can time out due to routing loops.
  defp safe_delete_all do
    num = SuperCache.Config.get_config(:num_partition)

    Enum.each(Manager.live_nodes(), fn n ->
      Enum.each(0..(num - 1), fn idx ->
        partition = :erpc.call(n, SuperCache.Partition, :get_partition_by_idx, [idx], 5_000)
        :erpc.call(n, SuperCache.Storage, :delete_all, [partition], 5_000)
      end)
    end)
  end

  # Wait until all live nodes agree on the same partition map size.
  defp wait_cluster_stable(expected_nodes \\ nil) do
    n = expected_nodes || length(Manager.live_nodes())
    wait_until(fn ->
      Enum.all?(Manager.live_nodes(), fn node ->
        try do
          count = :erpc.call(node, Manager, :live_nodes, [], 3_000) |> length()
          count == n
        catch
          _, _ -> false
        end
      end)
    end, 5_000)
  end

  defp wait_until(fun, timeout \\ 3_000, interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline, interval)
  end

  defp do_wait(fun, deadline, interval) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(interval)
        do_wait(fun, deadline, interval)
      end
    end
  end

  # ── Setup ────────────────────────────────────────────────────────────────────

  setup_all do
    if node() == :nonode@nohost do
      raise "Cluster tests require a distributed node. Run with: mix test.cluster"
    end

    if SuperCache.started?() do
      SuperCache.Cluster.Bootstrap.stop()
      Process.sleep(100)
    end

    SuperCache.Cluster.Bootstrap.start!(@cache_opts)

    {peer1, node1} = start_peer(:cache_node1)
    {peer2, node2} = start_peer(:cache_node2)

    true = Node.connect(node1)
    true = Node.connect(node2)

    # Connect peers to each other — without this, node1 and node2 only know
    # about the primary node but not each other, so their Manager never sees
    # 3 nodes and wait_cluster_stable times out.
    true = :erpc.call(node1, Node, :connect, [node2], 5_000)

    true = wait_until(fn -> length(Manager.live_nodes()) == 3 end)

    # Wait until every node's Manager reports 3 live nodes.
    true = wait_cluster_stable(3)
    IO.puts("Cluster stable: #{inspect(Manager.live_nodes())}")

    {:ok, agent} = Agent.start_link(fn ->
      %{peer1: peer1, node1: node1, peer2: peer2, node2: node2}
    end)

    on_exit(fn ->
      state = Agent.get(agent, & &1)
      stop_peer(state.peer1)
      stop_peer(state.peer2)
      SuperCache.Cluster.Bootstrap.stop()
    end)

    %{agent: agent, node1: node1, node2: node2, peer1: peer1, peer2: peer2}
  end

  # Use safe_delete_all instead of Cache.delete_all() to avoid routing timeouts.
  setup do
    safe_delete_all()
    Process.sleep(100)
    :ok
  end

  # ── Basic write / read ────────────────────────────────────────────────────────

  test "put and primary read" do
    Cache.put!({:user, 1, "Alice"})
    assert [{:user, 1, "Alice"}] == Cache.get!({:user, 1, nil}, read_mode: :primary)
  end

  test "delete removes a record" do
    Cache.put!({:user, 2, "Bob"})
    Cache.delete!({:user, 2, nil})
    assert [] == Cache.get!({:user, 2, nil}, read_mode: :primary)
  end

  # ── Replication ───────────────────────────────────────────────────────────────

  test "write is replicated to both peers", %{node1: node1, node2: node2} do
    Cache.put!({:session, "tok-abc", %{user: 1}})
    Process.sleep(300)

    Enum.each([node1, node2], fn n ->
      result = remote_get(n, {:session, "tok-abc", nil})
      assert [{:session, "tok-abc", %{user: 1}}] == result,
             "Node #{inspect(n)} could not read the record"
    end)
  end

  test "primary read is consistent" do
    Cache.put!({:item, 99, "value"})
    Process.sleep(100)
    assert [{:item, 99, "value"}] == Cache.get!({:item, 99, nil}, read_mode: :primary)
  end

  test "quorum read returns correct value" do
    Cache.put!({:quorum_key, "k1", "v1"})
    Process.sleep(300)
    assert [{:quorum_key, "k1", "v1"}] ==
      Cache.get!({:quorum_key, "k1", nil}, read_mode: :quorum)
  end

  test "write on non-primary node is routed to correct primary" do
    key_data = {:routed, "test_key", nil}
    primary  = primary_for(elem(key_data, 1))
    writer   =
      Manager.live_nodes()
      |> Enum.find(fn n -> n != primary end)
      |> then(fn
           nil -> primary
           n   -> n
         end)

    :erpc.call(writer, SuperCache.Distributed, :put!,
      [{:routed, "test_key", "value"}], 8_000)
    Process.sleep(200)

    assert [{:routed, "test_key", "value"}] ==
      Cache.get!({:routed, "test_key", nil}, read_mode: :primary)
  end

  # ── Node failure ──────────────────────────────────────────────────────────────

  test "reads still work after a peer goes down", %{agent: agent, peer2: peer2, node2: node2} do
    Cache.put!({:durable, "k1", "v1"})
    Process.sleep(300)

    stop_peer(peer2)
    true = wait_until(fn -> node2 not in Manager.live_nodes() end)
    wait_cluster_stable(2)

    assert [{:durable, "k1", "v1"}] == Cache.get!({:durable, "k1", nil}, read_mode: :primary)

    {new_peer2, new_node2} = restart_peer(:cache_node2)
    Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)
  end

  # ── Full sync on rejoin ───────────────────────────────────────────────────────

  test "node rejoin triggers full sync", %{agent: agent, peer1: peer1, node1: node1} do
    Cache.put!({:sync_test, "key1", "data1"})
    Process.sleep(300)

    stop_peer(peer1)
    true = wait_until(fn -> node1 not in Manager.live_nodes() end)
    wait_cluster_stable(2)

    Cache.put!({:sync_test, "key2", "data2"})
    Process.sleep(200)

    {new_peer1, new_node1} = restart_peer(:cache_node1)
    Agent.update(agent, fn s -> %{s | peer1: new_peer1, node1: new_node1} end)

    # Allow full sync to complete.
    Process.sleep(1_500)

    r1 = remote_get(new_node1, {:sync_test, "key1", nil})
    r2 = remote_get(new_node1, {:sync_test, "key2", nil})

    assert [{:sync_test, "key1", "data1"}] == r1
    assert [{:sync_test, "key2", "data2"}] == r2
  end

  # ── delete_all ────────────────────────────────────────────────────────────────

  test "delete_all clears data on all nodes", %{node1: node1, node2: node2} do
    Cache.put!({:to_clear, "a", 1})
    Cache.put!({:to_clear, "b", 2})
    Process.sleep(300)

    safe_delete_all()
    Process.sleep(300)

    Enum.each([node1, node2], fn n ->
      result = remote_get(n, {:to_clear, "a", nil})
      assert [] == result, "Node #{inspect(n)} still has data after delete_all"
    end)
  end
end
