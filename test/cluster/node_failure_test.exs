defmodule SuperCache.Cluster.NodeFailureTest do
  @moduledoc """
  Tests for cluster resilience:

  - Reads survive a peer going down (failover to replica)
  - Full sync on node rejoin
  - delete_all propagates to all nodes
  """

  use SuperCache.ClusterCase

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    setup_cluster!(:failure_peer1, :failure_peer2)
  end

  # ── Node failure ──────────────────────────────────────────────────────────────

  test "reads still work after a peer goes down", %{agent: agent, peer2: peer2, node2: node2} do
    Cache.put!({:durable, "k1", "v1"})
    order = SuperCache.Partition.get_partition_order(:durable)
    {primary, replicas} = Manager.get_replicas(order)

    assert_node_has(primary, :durable, :durable, [{:durable, "k1", "v1"}], 1_000)

    if node2 in replicas do
      assert_node_has(node2, :durable, :durable, [{:durable, "k1", "v1"}], 2_000)
    end

    stop_peer(peer2)

    assert wait_until(fn -> node2 not in Manager.live_nodes() end, 5_000),
           "node2 did not leave live_nodes after stop"

    wait_cluster_stable(2)

    assert [{:durable, "k1", "v1"}] == Cache.get!({:durable, "k1", nil}, read_mode: :primary)

    {new_peer2, new_node2} = restart_peer(:failure_peer2)
    Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)

    node1 = Agent.get(agent, & &1.node1)
    restart_node_monitor(nodes: [node1, new_node2])
    wait_cluster_stable(3)
  end

  # ── Full sync on rejoin ───────────────────────────────────────────────────────

  test "node rejoin triggers full sync", %{agent: agent, peer1: peer1, node1: node1} do
    Cache.put!({:sync_key1, "data1"})

    order1 = SuperCache.Partition.get_partition_order(:sync_key1)
    {primary1, _} = Manager.get_replicas(order1)
    assert_node_has(primary1, :sync_key1, :sync_key1, [{:sync_key1, "data1"}], 1_000)

    stop_peer(peer1)

    assert wait_until(fn -> node1 not in Manager.live_nodes() end, 5_000),
           "node1 did not leave live_nodes after stop"

    wait_cluster_stable(2)

    # Write while node1 is absent so the rejoin sync must deliver this entry.
    Cache.put!({:sync_key2, "data2"})
    order2 = SuperCache.Partition.get_partition_order(:sync_key2)
    {primary2, _} = Manager.get_replicas(order2)
    assert_node_has(primary2, :sync_key2, :sync_key2, [{:sync_key2, "data2"}], 1_000)

    {new_peer1, new_node1} = restart_peer(:failure_peer1)
    Agent.update(agent, fn s -> %{s | peer1: new_peer1, node1: new_node1} end)

    # Allow the full sync to complete.
    Process.sleep(2_000)

    assert [{:sync_key1, "data1"}] == Cache.get!({:sync_key1, nil}, read_mode: :primary)
    assert [{:sync_key2, "data2"}] == Cache.get!({:sync_key2, nil}, read_mode: :primary)

    num = SuperCache.Config.get_config(:num_partition)

    node1_orders =
      Enum.filter(0..(num - 1), fn o ->
        {p, rs} = Manager.get_replicas(o)
        new_node1 in [p | rs]
      end)

    for {key, val} <- [{:sync_key1, "data1"}, {:sync_key2, "data2"}] do
      ord = SuperCache.Partition.get_partition_order(key)

      if ord in node1_orders do
        assert_node_has(new_node1, key, key, [{key, val}], 1_000)
      end
    end

    node2 = Agent.get(agent, & &1.node2)
    restart_node_monitor(nodes: [new_node1, node2])
    wait_cluster_stable(3)
  end

  # ── delete_all ────────────────────────────────────────────────────────────────

  test "delete_all clears data on all nodes", %{node1: node1, node2: node2} do
    Cache.put!({:to_clear, "a", 1})
    Cache.put!({:to_clear_b, "b", 2})

    {pa, _} = Manager.get_replicas(SuperCache.Partition.get_partition_order(:to_clear))
    {pb, _} = Manager.get_replicas(SuperCache.Partition.get_partition_order(:to_clear_b))
    assert_node_has(pa, :to_clear, :to_clear, [{:to_clear, "a", 1}], 1_000)
    assert_node_has(pb, :to_clear_b, :to_clear_b, [{:to_clear_b, "b", 2}], 1_000)

    safe_delete_all()
    Process.sleep(300)

    all_nodes = [node() | Manager.live_nodes()] |> Enum.uniq()

    for n <- all_nodes, key <- [:to_clear, :to_clear_b] do
      got = node_read(n, key, key)

      assert [] == got,
             "Node #{inspect(n)} still has #{inspect(key)} after delete_all: #{inspect(got)}"
    end

    _ = node1
    _ = node2
  end
end
