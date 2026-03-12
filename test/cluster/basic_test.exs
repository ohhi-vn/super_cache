defmodule SuperCache.Cluster.BasicTest do
  @moduledoc """
  Smoke tests for the distributed cache:

  - Single-node write / primary read
  - Delete
  - Replication to all replica nodes
  - Quorum read
  - Write routing from a non-primary node
  """

  use SuperCache.ClusterCase

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    setup_cluster!(:basic_peer1, :basic_peer2)
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
    expected = [{:session, "tok-abc", %{user: 1}}]

    order = SuperCache.Partition.get_partition_order(:session)
    {primary, replicas} = Manager.get_replicas(order)
    holders = [primary | replicas]

    assert_node_has(primary, :session, :session, expected, 500)

    Enum.each(replicas, fn r ->
      assert_node_has(r, :session, :session, expected, 2_000)
    end)

    assert Enum.any?([node1, node2], fn n -> n in holders end),
           "Neither node1 nor node2 is primary/replica for :session. " <>
             "Holders: #{inspect(holders)}"
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
    primary  = primary_for(elem(key_data, 0))

    writer =
      Manager.live_nodes()
      |> Enum.find(fn n -> n != primary end)
      |> then(fn
        nil -> primary
        n   -> n
      end)

    :erpc.call(writer, SuperCache.Distributed, :put!, [{:routed, "test_key", "value"}], 8_000)
    Process.sleep(200)

    assert [{:routed, "test_key", "value"}] ==
             Cache.get!({:routed, "test_key", nil}, read_mode: :primary)
  end
end
