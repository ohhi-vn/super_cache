defmodule SuperCache.Cluster.NetworkPartitionTest do
  @moduledoc """
  Network-partition integration tests.

  The peer VMs share one host, so dropping their Erlang distribution links
  also kills the `:peer` control channel. Instead, each test narrows its
  `NodeMonitor` managed set to the nodes that would still be reachable across
  the simulated network split. `NodeMonitor` then drives `Manager.node_down/1`
  through exactly the same path a real `:nodedown` event does: partition maps
  are rebuilt, replication stops targeting unreachable hosts, and rejoining
  announces `node_up/1` and triggers a full sync.

  Three shapes are covered:

  - an asymmetric split, where two replicas lose sight of each other but the
    third node can still reach both;
  - a symmetric split, where one peer is isolated from the other two;
  - a full split into three singleton groups.
  """

  use SuperCache.ClusterCase

  alias SuperCache.Cluster.NodeMonitor
  alias SuperCache.Cluster.Router

  setup_all do
    setup_cluster!(:partition_peer1, :partition_peer2)
  end

  setup %{node1: node1, node2: node2} do
    on_exit(fn ->
      restore_view(node(), [node(), node1, node2])
      restore_view(node1, [node(), node1, node2])
      restore_view(node2, [node(), node1, node2])
      await_convergence(15_000)
    end)

    :ok
  end

  # ── Asymmetric partition ─────────────────────────────────────────────────────

  test "a partition between two replicas still lets the connected node replicate", %{
    node1: node1,
    node2: node2
  } do
    set_view(node1, [node(), node1])
    set_view(node2, [node(), node2])

    assert wait_until(
             fn ->
               node2 not in live_on(node1) and node1 not in live_on(node2)
             end,
             10_000
           ),
           "the two peers did not observe their partition"

    # This node still sees both peers, so it remains the bridge.
    assert live_on(node()) == Enum.sort([node(), node1, node2])

    # node1 and node2 cannot reach each other: writes primaryed here still
    # replicate to node1, while writes primaryed on node1 reach node1 but not
    # its replica node2 until the link is restored.
    local_key = key_with_primary(:asymmetric_local, node())
    remote_key = key_with_primary(:asymmetric_remote, node1)

    assert true = SuperCache.put!({local_key, "value"})
    assert true = SuperCache.put!({remote_key, "value"})

    assert_node_has(node1, local_key, local_key, [{local_key, "value"}], 5_000)
    assert_node_has(node1, remote_key, remote_key, [{remote_key, "value"}], 5_000)
    assert node_read(node2, remote_key, remote_key) == []

    restore_view(node1, [node(), node1, node2])
    restore_view(node2, [node(), node1, node2])
    assert await_convergence(15_000)

    assert_node_has(node2, remote_key, remote_key, [{remote_key, "value"}], 15_000)
  end

  # ── Symmetric split ──────────────────────────────────────────────────────────

  test "an isolated peer serves stale local reads and resyncs when the partition heals", %{
    node1: node1,
    node2: node2
  } do
    key = :isolated_partition_key
    partition = SuperCache.Partition.get_partition_order(key)

    # Pick a key this peer holds before the split so its stale copy is real.
    replicas = Manager.get_replicas(partition) |> Tuple.to_list() |> List.flatten()
    unless node2 in replicas, do: flunk("test key is not stored on the isolated peer")

    assert true = SuperCache.put!({key, "before"})
    assert_node_has(node2, key, key, [{key, "before"}], 5_000)

    set_view(node(), [node(), node1])
    set_view(node1, [node(), node1])
    set_view(node2, [node2])

    assert wait_until(
             fn ->
               live_on(node()) == Enum.sort([node(), node1]) and
                 live_on(node1) == Enum.sort([node(), node1]) and
                 live_on(node2) == [node2]
             end,
             10_000
           ),
           "cluster did not split into {majority, isolated peer}"

    # A write on the majority side cannot reach the isolated peer.
    assert true = SuperCache.put!({key, "after"})
    assert_node_has(node1, key, key, [{key, "after"}], 5_000)

    assert :erpc.call(node2, Router, :local_read, [partition, :get, key], 5_000) == [
             {key, "before"}
           ],
           "isolated peer should still serve its pre-partition value locally"

    restore_view(node(), [node(), node1, node2])
    restore_view(node1, [node(), node1, node2])
    restore_view(node2, [node(), node1, node2])
    assert await_convergence(15_000)

    assert_node_has(node2, key, key, [{key, "after"}], 15_000)

    for target <- [node(), node1, node2] do
      assert live_on(target) == Enum.sort([node(), node1, node2])
    end
  end

  test "writes during a partition are verified on the majority and after heal", %{
    node1: node1,
    node2: node2
  } do
    set_view(node(), [node(), node1])
    set_view(node1, [node(), node1])
    set_view(node2, [node2])

    assert wait_until(
             fn ->
               live_on(node2) == [node2] and node2 not in live_on(node())
             end,
             10_000
           )

    keys =
      0..12
      |> Enum.map(&:"partition_data_#{&1}")
      |> Enum.filter(fn key ->
        {primary, _} = Manager.get_replicas(SuperCache.Partition.get_partition_order(key))
        primary == node()
      end)
      |> Enum.take(5)

    assert length(keys) == 5, "could not find five keys primaryed on this node"

    for key <- keys do
      assert true = SuperCache.put!({key, "written-during-partition"})
    end

    # Every write must be readable on the majority side while the split is in
    # effect. Isolation of the peer is covered by the stale-read test above.
    for key <- keys do
      assert_node_has(node(), key, key, [{key, "written-during-partition"}], 5_000)
      assert_node_has(node1, key, key, [{key, "written-during-partition"}], 5_000)
    end

    restore_view(node(), [node(), node1, node2])
    restore_view(node1, [node(), node1, node2])
    restore_view(node2, [node(), node1, node2])
    assert await_convergence(15_000)

    # After the heal, each key must be present on every node that owns a copy
    # in the restored partition map.
    for key <- keys do
      {primary, replicas} = Manager.get_replicas(SuperCache.Partition.get_partition_order(key))

      for target <- Enum.uniq([primary | replicas]) do
        assert_node_has(target, key, key, [{key, "written-during-partition"}], 15_000)
      end
    end
  end

  test "a write on the isolated side is verified locally and survives the heal", %{
    node1: node1,
    node2: node2
  } do
    set_view(node(), [node(), node1])
    set_view(node1, [node(), node1])
    set_view(node2, [node2])

    assert wait_until(fn -> live_on(node2) == [node2] end, 10_000)

    key = :isolated_side_write

    # With only itself in its partition map, the isolated peer is the primary
    # for every partition and can still commit the write locally.
    assert true = :erpc.call(node2, SuperCache, :put!, [{key, "isolated-value"}], 5_000)
    assert node_read(node2, key, key) == [{key, "isolated-value"}]

    restore_view(node(), [node(), node1, node2])
    restore_view(node1, [node(), node1, node2])
    restore_view(node2, [node(), node1, node2])
    assert await_convergence(15_000)

    assert_node_has(node2, key, key, [{key, "isolated-value"}], 15_000)
  end

  test "membership converges again after a full three-way split", %{node1: node1, node2: node2} do
    set_view(node(), [node()])
    set_view(node1, [node1])
    set_view(node2, [node2])

    assert wait_until(
             fn ->
               live_on(node()) == [node()] and live_on(node1) == [node1] and
                 live_on(node2) == [node2]
             end,
             10_000
           ),
           "cluster did not split into three singleton groups"

    restore_view(node(), [node(), node1, node2])
    restore_view(node1, [node(), node1, node2])
    restore_view(node2, [node(), node1, node2])

    assert await_convergence(15_000)

    for target <- [node(), node1, node2] do
      assert live_on(target) == Enum.sort([node(), node1, node2])
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp set_view(target, nodes) when target == node() do
    :ok = NodeMonitor.reconfigure(nodes: nodes)
  end

  defp set_view(target, nodes) do
    :ok = :erpc.call(target, NodeMonitor, :reconfigure, [[nodes: nodes]], 10_000)
  end

  defp restore_view(target, nodes), do: set_view(target, nodes)

  defp live_on(target) when target == node(), do: Manager.live_nodes() |> Enum.sort()

  defp live_on(target) do
    :erpc.call(target, Manager, :live_nodes, [], 5_000) |> Enum.sort()
  end

  defp key_with_primary(prefix, primary) do
    0..100
    |> Enum.map(&:"#{prefix}_#{&1}")
    |> Enum.find(fn key ->
      {found, _replicas} = Manager.get_replicas(SuperCache.Partition.get_partition_order(key))
      found == primary
    end) || flunk("no partition is primaryed by #{inspect(primary)}")
  end
end
