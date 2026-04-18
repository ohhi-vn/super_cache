defmodule SuperCache.Cluster.ConfigConsistencyTest do
  @moduledoc """
  Verifies that every node in the cluster — including one that joins after
  the others are already live — holds an identical configuration and an
  identical view of the partition → primary/replica mapping.

  Three scenarios:
    1. All existing nodes agree on every config key.
    2. A freshly-started node automatically receives the correct config via
       `Bootstrap.start!/1` (each node calls it independently with the same
       opts).
    3. The partition map is identical on every node once the cluster is
       stable, because the algorithm is deterministic and uses the same
       sorted node list everywhere.
  """

  use SuperCache.ClusterCase

  # Structural keys that must be identical on every cluster node.
  # Matches Bootstrap.@config_keys — kept in sync manually.
  # :started / :cluster / :table_prefix are excluded for the same reasons
  # as in Bootstrap (liveness flag, constant, and crash-detectable
  # respectively).
  @config_keys [
    :key_pos,
    :partition_pos,
    :num_partition,
    :table_type,
    :replication_factor,
    :replication_mode
  ]

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    setup_cluster!(:cfg_peer1, :cfg_peer2)
  end

  # Each test in this file connects to peers directly via :erpc; make sure
  # they are reachable and in Manager before every test runs.
  setup %{agent: agent} do
    # Always read fresh refs from the agent — a prior test (e.g. the
    # late-joining test) may have restarted a peer and updated the agent.
    %{node1: node1, node2: node2} = Agent.get(agent, & &1)

    # Re-establish the static list so node_up events flow to Manager even if
    # a prior test left the monitor in a different mode.
    restart_node_monitor(nodes: [node1, node2])

    # Reconnect at the Erlang level in case the distribution link was lost.
    Node.connect(node1)
    Node.connect(node2)

    # If a peer is dead (e.g. a prior test stopped it and failed before
    # restoring it), restart it and update the agent.
    for {name, node_ref, peer_key, node_key} <- [
          {:cfg_peer1, node1, :peer1, :node1},
          {:cfg_peer2, node2, :peer2, :node2}
        ] do
      peer_alive =
        try do
          :erpc.call(node_ref, :erlang, :node, [], 2_000)
          true
        catch
          _, _ -> false
        end

      unless peer_alive do
        {new_peer, new_node} = restart_peer(name)
        Agent.update(agent, &Map.put(&1, peer_key, new_peer))
        Agent.update(agent, &Map.put(&1, node_key, new_node))
      end
    end

    # Re-read fresh refs in case restart_peer updated them.
    %{node1: fresh_node1, node2: fresh_node2} = Agent.get(agent, & &1)

    # Reconnect with the (possibly new) nodes.
    Node.connect(fresh_node1)
    Node.connect(fresh_node2)
    restart_node_monitor(nodes: [fresh_node1, fresh_node2])

    assert wait_until(
             fn -> fresh_node1 in Node.list() and fresh_node2 in Node.list() end,
             10_000
           ),
           "node1 and node2 must be Erlang-connected"

    assert wait_until(
             fn ->
               fresh_node1 in Manager.live_nodes() and fresh_node2 in Manager.live_nodes()
             end,
             10_000
           ),
           "node1 and node2 must be in Manager.live_nodes"

    assert wait_cluster_stable(3),
           "Cluster not stable entering config consistency tests. " <>
             "live_nodes: #{inspect(Manager.live_nodes())}"

    # Verify both peers are reachable at the :erpc level.
    for peer <- [fresh_node1, fresh_node2] do
      try do
        :erpc.call(peer, SuperCache.Cluster.Bootstrap, :running?, [], 5_000)
      catch
        _, _ ->
          flunk("Peer #{inspect(peer)} is not reachable before config consistency tests")
      end
    end

    {:ok, node1: fresh_node1, node2: fresh_node2}
  end

  # ── Private helpers ───────────────────────────────────────────────────────────

  # Read the structural config from `target_node` via Bootstrap.export_config/0.
  # No closures cross the :erpc boundary.
  defp remote_config(target_node) do
    :erpc.call(target_node, SuperCache.Cluster.Bootstrap, :export_config, [], 5_000)
  end

  # Read the full partition map from `target_node` via
  # Bootstrap.fetch_partition_map/1.  Node.connect/1 is called defensively
  # before :erpc in case a prior test restarted a peer and the distribution
  # link has not been re-established yet.
  defp remote_partition_map(target_node) do
    Node.connect(target_node)
    num = SuperCache.Config.get_config(:num_partition)

    :erpc.call(target_node, SuperCache.Cluster.Bootstrap, :fetch_partition_map, [num], 5_000)
    |> Map.new()
  end

  # ── Tests ─────────────────────────────────────────────────────────────────────

  test "all nodes agree on every config key", %{node1: node1, node2: node2} do
    local_cfg = Map.new(@config_keys, fn k -> {k, SuperCache.Config.get_config(k)} end)

    for peer <- [node1, node2] do
      peer_cfg = remote_config(peer)

      assert local_cfg == peer_cfg,
             "Config mismatch on #{inspect(peer)}.\n" <>
               "  local:  #{inspect(local_cfg)}\n" <>
               "  remote: #{inspect(peer_cfg)}"
    end
  end

  test "partition map is identical on all nodes", %{node1: node1, node2: node2} do
    local_map = remote_partition_map(node())

    for peer <- [node1, node2] do
      peer_map = remote_partition_map(peer)

      assert local_map == peer_map,
             "Partition map mismatch on #{inspect(peer)}.\n" <>
               "  expected: #{inspect(local_map)}\n" <>
               "  got:      #{inspect(peer_map)}"
    end
  end

  test "late-joining node receives correct config and consistent partition map",
       %{agent: agent, node1: node1, node2: node2} do
    # Temporarily drop to a 2-node cluster so the late-joiner arrives into a
    # stable (not transient) topology.
    {peer2, _node2} = {Agent.get(agent, & &1.peer2), node2}
    stop_peer(peer2)

    assert wait_until(fn -> node2 not in Manager.live_nodes() end, 5_000),
           "node2 did not leave live_nodes before late-joiner test"

    wait_cluster_stable(2)

    # Start a brand-new node that has never been part of this cluster.
    {:ok, late_peer, late_node} =
      :peer.start(%{
        name: :cfg_late_joiner,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie",
          :erlang.atom_to_list(:erlang.get_cookie()),
          ~c"-connect_all",
          ~c"false"
        ]
      })

    on_exit(fn -> stop_peer(late_peer) end)

    :erpc.call(late_node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(late_node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(late_node, Application, :ensure_all_started, [:super_cache])

    # Bootstrap with IDENTICAL opts — the operator contract.
    :erpc.call(late_node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])

    true = Node.connect(late_node)
    :erpc.call(late_node, Node, :connect, [node1])

    # NodeMonitor is in static mode with managed = [node1, node2]; late_node
    # is not in that set, so the kernel :nodeup event is filtered.  Notify
    # Manager directly on BOTH the local node and node1 — identical pattern
    # to restart_peer/1.  Without notifying node1, the convergence check
    # below will fail because node1 never sees the late-joiner.
    Manager.node_up(late_node)
    :erpc.call(node1, Manager, :node_up, [late_node], 5_000)

    # Also reconfigure node1's NodeMonitor to include the late-joiner so
    # it forwards future :nodeup/:nodedown events for it.
    :erpc.call(
      node1,
      NodeMonitor,
      :reconfigure,
      [[nodes: [node(), node2, late_node]]],
      5_000
    )

    # Reconfigure the local NodeMonitor to include the late-joiner as well.
    restart_node_monitor(nodes: [node1, node2, late_node])

    assert wait_until(fn -> late_node in Manager.live_nodes() end, 8_000),
           "Late-joining node #{inspect(late_node)} never appeared in Manager.live_nodes"

    # Wait until ALL three nodes agree on the same live set (not just the same
    # count) before asserting partition maps.  Divergent membership leads to
    # divergent maps even when node counts match.
    expected_set = MapSet.new([node(), node1, late_node])

    assert wait_until(
             fn ->
               Enum.all?([node(), node1, late_node], fn n ->
                 try do
                   MapSet.new(:erpc.call(n, Manager, :live_nodes, [], 3_000)) == expected_set
                 catch
                   _, _ -> false
                 end
               end)
             end,
             10_000
           ),
           "Nodes did not converge on live set #{inspect(MapSet.to_list(expected_set))} within 10 s"

    # 1. Config keys must match the reference node exactly.
    expected_cfg = Map.new(@config_keys, fn k -> {k, SuperCache.Config.get_config(k)} end)
    late_cfg = remote_config(late_node)

    assert expected_cfg == late_cfg,
           "Late-joiner #{inspect(late_node)} has wrong config.\n" <>
             "  expected: #{inspect(expected_cfg)}\n" <>
             "  got:      #{inspect(late_cfg)}"

    # 2. :started must be true — Bootstrap completed successfully.
    assert :erpc.call(late_node, SuperCache.Config, :get_config, [:started], 5_000) == true,
           "Late-joiner #{inspect(late_node)} did not complete Bootstrap (started != true)"

    # 3. Partition map must be identical on all three nodes.
    local_map = remote_partition_map(node())
    node1_map = remote_partition_map(node1)
    late_map = remote_partition_map(late_node)

    assert local_map == node1_map,
           "Partition map mismatch between test node and node1 after late join"

    assert local_map == late_map,
           "Partition map mismatch between test node and late-joiner #{inspect(late_node)}"

    # 4. The late-joiner must appear as primary or replica in the map.
    assert Enum.any?(local_map, fn {_idx, {primary, replicas}} ->
             late_node in [primary | replicas]
           end),
           "Late-joiner #{inspect(late_node)} is not assigned to any partition"

    # 5. Writes after the late join must be readable on the late-joiner for
    #    partitions it owns (end-to-end smoke-test of the new config).
    Cache.put!({:late_join_key, "probe", "value"})
    Process.sleep(300)

    ord = SuperCache.Partition.get_partition_order(:late_join_key)
    {primary, replicas} = Manager.get_replicas(ord)
    holders = [primary | replicas]

    Enum.each(holders, fn holder ->
      assert_node_has(
        holder,
        :late_join_key,
        :late_join_key,
        [{:late_join_key, "probe", "value"}],
        2_000
      )
    end)

    if late_node in holders do
      assert_node_has(
        late_node,
        :late_join_key,
        :late_join_key,
        [{:late_join_key, "probe", "value"}],
        2_000
      )
    end

    # ── Restore two-node cluster for subsequent tests ──────────────────────────
    stop_peer(late_peer)

    # NodeMonitor is in static mode; late_node is not in the original managed
    # set, so :nodedown is filtered.  Notify Manager directly on both the
    # local node and node1 to avoid waiting for health-check retry exhaustion.
    Manager.node_down(late_node)
    :erpc.call(node1, Manager, :node_down, [late_node], 5_000)

    # Restore the NodeMonitor managed sets to the original 2-peer config.
    restart_node_monitor(nodes: [node1, node2])
    :erpc.call(node1, NodeMonitor, :reconfigure, [[nodes: [node(), node2]]], 5_000)

    assert wait_until(fn -> late_node not in Manager.live_nodes() end, 8_000),
           "late_node did not leave Manager.live_nodes within 8 s"

    {new_peer2, new_node2} = restart_peer(:cfg_peer2)
    Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)
    restart_node_monitor(nodes: [node1, new_node2])
    :erpc.call(node1, NodeMonitor, :reconfigure, [[nodes: [node(), new_node2]]], 5_000)
    wait_cluster_stable(3)
  end

  test "node started with mismatched num_partition is detectable", %{node1: node1} do
    # Bootstrap.verify_cluster_config!/1 enforces structural config consistency
    # at join time.  A node with wrong :num_partition cannot join — start!/1
    # raises ArgumentError before any ETS table is created.
    wrong_opts = Keyword.put(@cache_opts, :num_partition, 4)

    {:ok, rogue_peer, rogue_node} =
      :peer.start(%{
        name: :cfg_rogue_node,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie",
          :erlang.atom_to_list(:erlang.get_cookie()),
          ~c"-connect_all",
          ~c"false"
        ]
      })

    on_exit(fn -> stop_peer(rogue_peer) end)

    :erpc.call(rogue_node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(rogue_node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(rogue_node, Application, :ensure_all_started, [:super_cache])

    mismatch_msg =
      try do
        :erpc.call(rogue_node, SuperCache.Cluster.Bootstrap, :start!, [wrong_opts], 15_000)
        flunk("Expected Bootstrap.start!/1 to raise for mismatched num_partition")
      catch
        :error, %ArgumentError{message: msg} -> msg
        :error, {:exception, %ArgumentError{message: msg}, _} -> msg
      end

    assert mismatch_msg =~ ":num_partition",
           "Error message must mention :num_partition, got: #{mismatch_msg}"

    refute :erpc.call(rogue_node, SuperCache.Cluster.Bootstrap, :running?, [], 5_000),
           "running?() must be false after rejected start!/1"

    refute wait_until(fn -> rogue_node in Manager.live_nodes() end, 2_000),
           "Rogue node must not join Manager.live_nodes after rejected start!/1"

    stop_peer(rogue_peer)
    wait_cluster_stable()

    _ = node1
  end
end
