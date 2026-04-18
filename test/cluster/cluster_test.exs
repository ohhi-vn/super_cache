defmodule SuperCache.ClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :sequential
  @moduletag timeout: 120_000

  alias SuperCache.Cluster.Manager
  alias SuperCache.Cluster.NodeMonitor

  require Logger

  # Base cache opts — deliberately omit node-source keys (:nodes / :nodes_mfa /
  # :refresh_ms) so NodeMonitor starts in legacy :all mode.  Tests that need
  # a restricted managed set call restart_node_monitor/1 (or exercise
  # Bootstrap.start!/1 with the new keys directly — see the dedicated test).
  @cache_opts [
    key_pos: 0,
    partition_pos: 0,
    cluster: :distributed,
    replication_factor: 2,
    num_partition: 8,
    table_type: :set
  ]

  # ── Peer helpers ──────────────────────────────────────────────────────────────

  defp start_peer(name) do
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

    Process.sleep(1000)

    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])

    {peer, node}
  end

  # Start a peer that does NOT run SuperCache — used to verify NodeMonitor
  # ignores Erlang nodes that are not part of the cache cluster.
  defp start_plain_peer(name) do
    cookie = :erlang.get_cookie()

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

    existing = Manager.live_nodes() |> List.delete(node())

    Enum.each(existing, fn other ->
      try do
        :erpc.call(node, Node, :connect, [other], 5_000)
      catch
        _, _ -> :ok
      end
    end)

    Manager.node_up(node)

    assert wait_until(fn -> node in Manager.live_nodes() end, 15_000),
           "Restarted node #{inspect(node)} did not join Manager.live_nodes"

    wait_cluster_stable()
    {peer, node}
  end

  defp restart_node_monitor(opts) do
    NodeMonitor.reconfigure(opts)
  end

  # ── Read / assert helpers ─────────────────────────────────────────────────────

  defp node_read(target_node, key, partition_value) do
    order = SuperCache.Partition.get_partition_order(partition_value)

    :erpc.call(
      target_node,
      SuperCache.Cluster.Router,
      :local_read,
      [order, :get, key],
      8_000
    )
  end

  defp assert_node_has(target_node, key, partition_value, expected, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_node_has(target_node, key, partition_value, expected, deadline)
  end

  defp do_assert_node_has(target_node, key, partition_value, expected, deadline) do
    got = node_read(target_node, key, partition_value)

    cond do
      got == expected ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "Node #{inspect(target_node)} local ETS for #{inspect(key)} timed out.\n" <>
            "  expected: #{inspect(expected)}\n" <>
            "  got:      #{inspect(got)}"
        )

      true ->
        Process.sleep(50)
        do_assert_node_has(target_node, key, partition_value, expected, deadline)
    end
  end

  defp primary_for(partition_value) do
    idx = SuperCache.Partition.get_partition_order(partition_value)
    {primary, _} = Manager.get_replicas(idx)
    primary
  end

  defp safe_delete_all do
    num = SuperCache.Config.get_config(:num_partition)

    Enum.each(Manager.live_nodes(), fn n ->
      Enum.each(0..(num - 1), fn idx ->
        try do
          partition =
            :erpc.call(n, SuperCache.Partition, :get_partition_by_idx, [idx], 5_000)

          :erpc.call(n, SuperCache.Storage, :delete_all, [partition], 5_000)
        catch
          kind, reason ->
            Logger.debug(
              "safe_delete_all: skipping #{inspect(n)} partition #{idx} — " <>
                "#{inspect({kind, reason})}"
            )
        end
      end)
    end)
  end

  defp wait_cluster_stable(expected_nodes \\ nil) do
    n = expected_nodes || length(Manager.live_nodes())

    wait_until(
      fn ->
        Enum.all?(Manager.live_nodes(), fn node ->
          try do
            count = :erpc.call(node, Manager, :live_nodes, [], 3_000) |> length()
            count == n
          catch
            _, _ -> false
          end
        end)
      end,
      5_000
    )
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

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    if node() == :nonode@nohost do
      raise "Cluster tests require a distributed node. Run with: mix test.cluster"
    end

    {:ok, _} = Application.ensure_all_started(:super_cache)

    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!(@cache_opts)

    {peer1, node1} = start_peer(:cache_node1)
    {peer2, node2} = start_peer(:cache_node2)

    true = Node.connect(node1)
    true = Node.connect(node2)
    true = :erpc.call(node1, Node, :connect, [node2], 5_000)

    assert wait_until(
             fn ->
               try do
                 length(Manager.live_nodes()) == 3
               catch
                 _, _ -> false
               end
             end,
             10_000
           ),
           "Local Manager did not reach 3 live nodes within 10 s"

    restart_node_monitor(nodes: [node1, node2])
    :erpc.call(node1, NodeMonitor, :reconfigure, [[nodes: [node(), node2]]], 5_000)
    :erpc.call(node2, NodeMonitor, :reconfigure, [[nodes: [node(), node1]]], 5_000)

    assert wait_cluster_stable(3),
           "Cluster views did not converge to 3 nodes. " <>
             "Live nodes: #{inspect(Manager.live_nodes())}"

    IO.puts("Cluster stable: #{inspect(Manager.live_nodes())}")

    {:ok, agent} =
      Agent.start_link(fn ->
        %{peer1: peer1, node1: node1, peer2: peer2, node2: node2}
      end)

    on_exit(fn ->
      state = Agent.get(agent, & &1)
      stop_peer(state.peer1)
      stop_peer(state.peer2)

      try do
        SuperCache.stop()
      catch
        :exit, _ -> :ok
      end
    end)

    %{agent: agent, node1: node1, node2: node2, peer1: peer1, peer2: peer2}
  end

  setup %{agent: agent} do
    %{node1: node1, node2: node2, peer1: peer1, peer2: peer2} =
      Agent.get(agent, & &1)

    safe_delete_all()
    Process.sleep(100)

    {:ok, node1: node1, node2: node2, peer1: peer1, peer2: peer2}
  end

  # ── Basic write / read ────────────────────────────────────────────────────────

  test "put and primary read" do
    SuperCache.put!({:user, 1, "Alice"})
    assert [{:user, 1, "Alice"}] == SuperCache.get!({:user, 1, nil}, read_mode: :primary)
  end

  test "delete removes a record" do
    SuperCache.put!({:user, 2, "Bob"})
    SuperCache.delete!({:user, 2, nil})
    assert [] == SuperCache.get!({:user, 2, nil}, read_mode: :primary)
  end

  # ── Replication ───────────────────────────────────────────────────────────────

  test "write is replicated to both peers", %{node1: node1, node2: node2} do
    SuperCache.put!({:session, "tok-abc", %{user: 1}})
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
    SuperCache.put!({:item, 99, "value"})
    Process.sleep(100)
    assert [{:item, 99, "value"}] == SuperCache.get!({:item, 99, nil}, read_mode: :primary)
  end

  test "quorum read returns correct value" do
    SuperCache.put!({:quorum_key, "k1", "v1"})
    Process.sleep(1_000)

    assert [{:quorum_key, "k1", "v1"}] ==
             SuperCache.get!({:quorum_key, "k1", nil}, read_mode: :quorum)
  end

  test "write on non-primary node is routed to correct primary" do
    key_data = {:routed, "test_key", nil}
    primary = primary_for(elem(key_data, 0))

    writer =
      Manager.live_nodes()
      |> Enum.find(fn n -> n != primary end)
      |> then(fn
        nil -> primary
        n -> n
      end)

    :erpc.call(
      writer,
      SuperCache,
      :put!,
      [{:routed, "test_key", "value"}],
      8_000
    )

    Process.sleep(200)

    assert [{:routed, "test_key", "value"}] ==
             SuperCache.get!({:routed, "test_key", nil}, read_mode: :primary)
  end

  # ── Node failure ──────────────────────────────────────────────────────────────

  test "reads still work after a peer goes down", %{agent: agent, peer2: peer2, node2: node2} do
    SuperCache.put!({:durable, "k1", "v1"})
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

    assert [{:durable, "k1", "v1"}] == SuperCache.get!({:durable, "k1", nil}, read_mode: :primary)

    {new_peer2, new_node2} = restart_peer(:cache_node2)
    Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)

    node1 = Agent.get(agent, & &1.node1)
    restart_node_monitor(nodes: [node1, new_node2])
    wait_cluster_stable(3)
  end

  # ── Full sync on rejoin ───────────────────────────────────────────────────────

  test "node rejoin triggers full sync", %{agent: agent, peer1: peer1, node1: node1} do
    SuperCache.put!({:sync_key1, "data1"})

    order1 = SuperCache.Partition.get_partition_order(:sync_key1)
    {primary1, _} = Manager.get_replicas(order1)
    assert_node_has(primary1, :sync_key1, :sync_key1, [{:sync_key1, "data1"}], 1_000)

    stop_peer(peer1)

    assert wait_until(fn -> node1 not in Manager.live_nodes() end, 5_000),
           "node1 did not leave live_nodes after stop"

    wait_cluster_stable(2)

    SuperCache.put!({:sync_key2, "data2"})
    order2 = SuperCache.Partition.get_partition_order(:sync_key2)
    {primary2, _} = Manager.get_replicas(order2)
    assert_node_has(primary2, :sync_key2, :sync_key2, [{:sync_key2, "data2"}], 1_000)

    {new_peer1, new_node1} = restart_peer(:cache_node1)
    Agent.update(agent, fn s -> %{s | peer1: new_peer1, node1: new_node1} end)

    # Wait for the full sync to deliver data to the rejoined node.
    # After rejoin the partition map is rebuilt, so recompute the primary
    # for each key — the old primary may no longer be correct.  Use
    # assert_node_has (which polls) instead of a fixed sleep so the test
    # is resilient to variable sync latency.
    {new_primary1, _} =
      Manager.get_replicas(SuperCache.Partition.get_partition_order(:sync_key1))

    {new_primary2, _} =
      Manager.get_replicas(SuperCache.Partition.get_partition_order(:sync_key2))

    assert_node_has(new_primary1, :sync_key1, :sync_key1, [{:sync_key1, "data1"}], 5_000)
    assert_node_has(new_primary2, :sync_key2, :sync_key2, [{:sync_key2, "data2"}], 5_000)

    # Cross-check via the public API as well.
    assert [{:sync_key1, "data1"}] == SuperCache.get!({:sync_key1, nil}, read_mode: :primary)
    assert [{:sync_key2, "data2"}] == SuperCache.get!({:sync_key2, nil}, read_mode: :primary)

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
    SuperCache.put!({:to_clear, "a", 1})
    SuperCache.put!({:to_clear_b, "b", 2})

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

  # ═════════════════════════════════════════════════════════════════════════════
  # Config consistency tests
  # ═════════════════════════════════════════════════════════════════════════════

  @config_keys [
    :key_pos,
    :partition_pos,
    :num_partition,
    :table_type,
    :replication_factor,
    :replication_mode
  ]

  describe "config consistency" do
    setup %{agent: agent} do
      # Always read fresh refs from the agent — a prior test (e.g. the
      # late-joining test) may have restarted a peer and updated the agent.
      %{node1: node1, node2: node2, peer1: peer1, peer2: peer2} = Agent.get(agent, & &1)

      # If a peer is dead (e.g. a prior test stopped it and failed before
      # restoring it), restart it and update the agent.
      for {name, node_ref, peer_key, node_key} <- [
            {:cache_node1, node1, :peer1, :node1},
            {:cache_node2, node2, :peer2, :node2}
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

      # Reconnect at the Erlang level in case the distribution link was lost.
      Node.connect(fresh_node1)
      Node.connect(fresh_node2)

      restart_node_monitor(nodes: [fresh_node1, fresh_node2])

      assert wait_until(
               fn -> fresh_node1 in Node.list() and fresh_node2 in Node.list() end,
               10_000
             ),
             "config consistency setup: node1 and node2 must be Erlang-connected"

      assert wait_until(
               fn ->
                 fresh_node1 in Manager.live_nodes() and fresh_node2 in Manager.live_nodes()
               end,
               10_000
             ),
             "config consistency setup: node1 and node2 must be in Manager.live_nodes"

      {:ok, node1: fresh_node1, node2: fresh_node2}
    end

    defp remote_config(target_node) do
      :erpc.call(target_node, SuperCache.Cluster.Bootstrap, :export_config, [], 5_000)
    end

    defp remote_partition_map(target_node) do
      Node.connect(target_node)
      num = SuperCache.Config.get_config(:num_partition)

      :erpc.call(
        target_node,
        SuperCache.Cluster.Bootstrap,
        :fetch_partition_map,
        [num],
        5_000
      )
      |> Map.new()
    end

    setup %{node1: node1, node2: node2} do
      assert wait_cluster_stable(3),
             "Cluster not stable entering config consistency tests. " <>
               "live_nodes: #{inspect(Manager.live_nodes())}"

      for peer <- [node1, node2] do
        try do
          :erpc.call(peer, SuperCache.Cluster.Bootstrap, :running?, [], 5_000)
        catch
          _, _ ->
            flunk("Peer #{inspect(peer)} is not reachable before config consistency tests")
        end
      end

      :ok
    end

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
      {peer2, node2} = {Agent.get(agent, & &1.peer2), node2}
      stop_peer(peer2)

      assert wait_until(fn -> node2 not in Manager.live_nodes() end, 5_000),
             "node2 did not leave live_nodes before late-joiner test"

      wait_cluster_stable(2)

      {late_peer, late_node} =
        :peer.start(%{
          name: :late_joiner,
          host: ~c"127.0.0.1",
          args: [
            ~c"-setcookie",
            :erlang.atom_to_list(:erlang.get_cookie()),
            ~c"-connect_all",
            ~c"false"
          ]
        })
        |> then(fn {:ok, peer, node} -> {peer, node} end)

      on_exit(fn -> stop_peer(late_peer) end)

      :erpc.call(late_node, :code, :add_paths, [:code.get_path()])
      {:ok, _} = :erpc.call(late_node, Application, :ensure_all_started, [:logger])
      {:ok, _} = :erpc.call(late_node, Application, :ensure_all_started, [:super_cache])

      :erpc.call(late_node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])

      true = Node.connect(late_node)
      :erpc.call(late_node, Node, :connect, [node1])

      # NodeMonitor is in static mode; late_node is not in that set, so the
      # kernel :nodeup event is filtered.  Notify Manager directly on BOTH
      # the local node and node1, and reconfigure both NodeMonitors to
      # include the late-joiner so future events are forwarded.
      Manager.node_up(late_node)
      :erpc.call(node1, Manager, :node_up, [late_node], 5_000)

      :erpc.call(
        node1,
        NodeMonitor,
        :reconfigure,
        [[nodes: [node(), node2, late_node]]],
        5_000
      )

      restart_node_monitor(nodes: [node1, node2, late_node])

      assert wait_until(fn -> late_node in Manager.live_nodes() end, 8_000),
             "Late-joining node #{inspect(late_node)} never appeared in Manager.live_nodes"

      expected_set = MapSet.new([node(), node1, late_node])

      assert wait_until(
               fn ->
                 Enum.all?([node(), node1, late_node], fn n ->
                   try do
                     remote_live = :erpc.call(n, Manager, :live_nodes, [], 3_000)
                     MapSet.new(remote_live) == expected_set
                   catch
                     _, _ -> false
                   end
                 end)
               end,
               10_000
             ),
             "Nodes did not converge on live set #{inspect(MapSet.to_list(expected_set))} within 10 s"

      expected_cfg = Map.new(@config_keys, fn k -> {k, SuperCache.Config.get_config(k)} end)
      late_cfg = remote_config(late_node)

      assert expected_cfg == late_cfg,
             "Late-joiner #{inspect(late_node)} has wrong config.\n" <>
               "  expected: #{inspect(expected_cfg)}\n" <>
               "  got:      #{inspect(late_cfg)}"

      assert :erpc.call(late_node, SuperCache.Config, :get_config, [:started], 5_000) == true,
             "Late-joiner #{inspect(late_node)} did not complete Bootstrap (started != true)"

      local_map = remote_partition_map(node())
      node1_map = remote_partition_map(node1)
      late_map = remote_partition_map(late_node)

      assert local_map == node1_map,
             "Partition map mismatch between test node and node1 after late join"

      assert local_map == late_map,
             "Partition map mismatch between test node and late-joiner #{inspect(late_node)}"

      late_in_map? =
        Enum.any?(local_map, fn {_idx, {primary, replicas}} ->
          late_node in [primary | replicas]
        end)

      assert late_in_map?,
             "Late-joiner #{inspect(late_node)} is not assigned to any partition"

      SuperCache.put!({:late_join_key, "probe", "value"})
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

      stop_peer(late_peer)
      Manager.node_down(late_node)

      assert wait_until(fn -> late_node not in Manager.live_nodes() end, 8_000),
             "late_node did not leave Manager.live_nodes within 8 s"

      {new_peer2, new_node2} = restart_peer(:cache_node2)
      Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)
      restart_node_monitor(nodes: [node1, new_node2])
      wait_cluster_stable(3)
    end

    test "node started with mismatched num_partition is detectable", %{node1: node1} do
      wrong_opts = Keyword.put(@cache_opts, :num_partition, 4)

      {rogue_peer, rogue_node} =
        :peer.start(%{
          name: :rogue_node,
          host: ~c"127.0.0.1",
          args: [
            ~c"-setcookie",
            :erlang.atom_to_list(:erlang.get_cookie()),
            ~c"-connect_all",
            ~c"false"
          ]
        })
        |> then(fn {:ok, peer, node} -> {peer, node} end)

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
    end
  end

  # ═════════════════════════════════════════════════════════════════════════════
  # NodeMonitor-specific tests
  # ═════════════════════════════════════════════════════════════════════════════

  describe "NodeMonitor — option validation" do
    test "starts successfully with no options (legacy :all mode)" do
      restart_node_monitor([])
      assert Process.alive?(Process.whereis(NodeMonitor))
    end

    test "raises when :nodes is not a list of atoms" do
      assert_raise ArgumentError, ~r/:nodes/i, fn ->
        NodeMonitor.start_link(nodes: ["not_an_atom"])
      end
    end

    test "raises when :nodes_mfa is not a {m, f, a} tuple" do
      assert_raise ArgumentError, ~r/:nodes_mfa/i, fn ->
        NodeMonitor.start_link(nodes_mfa: :bad_value)
      end
    end

    test "Bootstrap.start!/1 with :nodes restricts NodeMonitor managed set",
         %{node1: node1, node2: node2} do
      # Stop and restart local Bootstrap with :nodes so NodeMonitor switches to
      # a static managed set.  A plain Erlang node that connects afterward must
      # NOT appear in Manager.live_nodes — NodeMonitor's watched?/2 gate should
      # filter it because it is not in the :nodes list.
      #
      # The on_exit callback restores the original :all-mode Bootstrap so
      # subsequent tests start from a clean state.
      on_exit(fn ->
        try do
          SuperCache.Cluster.Bootstrap.stop()
          SuperCache.Cluster.Bootstrap.start!(@cache_opts)
          restart_node_monitor(nodes: [node1, node2])
          wait_cluster_stable(3)
        catch
          _, _ -> :ok
        end
      end)

      SuperCache.Cluster.Bootstrap.stop()

      # Ensure node1 and node2 are still connected at the Erlang level
      # before restarting Bootstrap.  Bootstrap.stop() does not disconnect
      # peers, but the distribution link may have been lost if a prior test
      # disrupted the cluster.  Without an Erlang-level connection,
      # NodeMonitor.reconfigure cannot call Manager.node_up for the managed
      # peers, and Manager.live_nodes() will be missing them.
      Node.connect(node1)
      Node.connect(node2)

      # Pass :nodes = [node1, node2] so NodeMonitor's managed set is exactly
      # those two peers.  Any other Erlang node that connects is filtered.
      SuperCache.Cluster.Bootstrap.start!(@cache_opts ++ [nodes: [node1, node2]])

      # Confirm the local node is back up and the two managed peers are still live.
      assert wait_until(
               fn ->
                 node1 in Manager.live_nodes() and node2 in Manager.live_nodes()
               end,
               10_000
             ),
             "Managed peers must remain in Manager.live_nodes after Bootstrap restart"

      # Start a plain Erlang node — no SuperCache OTP app.
      {plain_peer, plain_node} = start_plain_peer(:bs_ns_plain_outsider)
      on_exit(fn -> stop_peer(plain_peer) end)

      # Connect at the Erlang level; this fires a :nodeup on the local NodeMonitor.
      # Because plain_node is not in the :nodes list, watched?/2 returns false and
      # Manager.node_up is never called.
      Node.connect(plain_node)
      Process.sleep(300)

      refute plain_node in Manager.live_nodes(),
             "Unmanaged plain_node #{inspect(plain_node)} was added to Manager.live_nodes " <>
               "— Bootstrap.start!/1 should have configured NodeMonitor to filter it. " <>
               "live_nodes: #{inspect(Manager.live_nodes())}"
    end
  end

  describe "NodeMonitor — static :nodes list" do
    test "non-managed :nodeup events are ignored", %{node1: node1, node2: node2} do
      {plain_peer, plain_node} = start_plain_peer(:unmanaged_node)

      on_exit(fn -> stop_peer(plain_peer) end)

      # Ensure managed nodes are connected before reconfiguring NodeMonitor.
      Node.connect(node1)
      Node.connect(node2)
      restart_node_monitor(nodes: [node1, node2])

      Node.connect(plain_node)
      Process.sleep(300)

      refute plain_node in Manager.live_nodes(),
             "Unmanaged node #{inspect(plain_node)} was added to Manager — " <>
               "NodeMonitor should have filtered it out."
    end

    test "non-managed :nodedown events are ignored", %{node1: node1, node2: node2} do
      {plain_peer, plain_node} = start_plain_peer(:unmanaged_down_node)

      on_exit(fn -> stop_peer(plain_peer) end)

      # Ensure node1 and node2 are connected before reconfiguring NodeMonitor.
      Node.connect(node1)
      Node.connect(node2)
      restart_node_monitor(nodes: [node1, node2])

      assert wait_until(
               fn -> node1 in Manager.live_nodes() and node2 in Manager.live_nodes() end,
               5_000
             ),
             "Precondition: node1 and node2 must be in Manager.live_nodes"

      Node.connect(plain_node)
      Process.sleep(100)

      stop_peer(plain_peer)
      Process.sleep(300)

      live = Manager.live_nodes()
      assert node1 in live
      assert node2 in live
    end

    test ":nodeup for a managed node is forwarded to Manager", %{
      agent: agent,
      node1: node1,
      node2: node2,
      peer1: peer1
    } do
      restart_node_monitor(nodes: [node1, node2])

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 5_000),
             "Precondition: node1 must be live before disconnect"

      # Stop the peer and wait for it to leave both Manager.live_nodes and
      # the Erlang connection mesh before restarting it.
      :peer.stop(peer1)

      assert wait_until(fn -> node1 not in Manager.live_nodes() end, 5_000),
             "node1 did not leave Manager.live_nodes after :peer.stop"

      assert wait_until(fn -> node1 not in Node.list() end, 5_000),
             "node1 did not disconnect from Erlang mesh within 5 s"

      # Start a fresh peer with the same short name.  start_peer connects
      # to the new node and calls Manager.node_up explicitly, but the
      # :nodeup kernel event is also forwarded by NodeMonitor because the
      # node is in the managed set.  Either path should result in the node
      # appearing in Manager.live_nodes.
      {new_peer1, new_node1} = start_peer(:cache_node1)
      Agent.update(agent, fn s -> %{s | peer1: new_peer1, node1: new_node1} end)

      assert wait_until(fn -> new_node1 in Manager.live_nodes() end, 10_000),
             ":nodeup for managed node1 was not forwarded to Manager"

      wait_cluster_stable(3)
    end

    test ":nodedown for a managed node is forwarded to Manager", %{
      agent: agent,
      peer2: peer2,
      node2: node2,
      node1: node1
    } do
      # Ensure nodes are connected before reconfiguring NodeMonitor.
      Node.connect(node1)
      Node.connect(node2)
      restart_node_monitor(nodes: [node1, node2])

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 5_000),
             "Precondition: node2 must be in Manager.live_nodes before stop"

      stop_peer(peer2)

      assert wait_until(fn -> node2 not in Manager.live_nodes() end, 5_000),
             "Manager still lists node2 after it went down"

      {new_peer2, new_node2} = restart_peer(:cache_node2)
      restart_node_monitor(nodes: [node1, new_node2])
      Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)
      wait_cluster_stable(3)
    end

    test "self (node()) is never in managed peers list", %{node1: node1, node2: node2} do
      on_exit(fn -> restart_node_monitor(nodes: [node1, node2]) end)

      restart_node_monitor(nodes: [node()])
      assert Process.whereis(NodeMonitor) != nil
    end
  end

  describe "NodeMonitor — :nodes_mfa dynamic source" do
    setup %{node1: node1, node2: node2} do
      # Ensure nodes are connected before any MFA reconfigure.
      Node.connect(node1)
      Node.connect(node2)

      {:ok, disco} = Agent.start_link(fn -> [] end, name: :test_disco)

      on_exit(fn ->
        try do
          NodeMonitor.reconfigure(nodes: [node1, node2])
        catch
          _, _ -> :ok
        end

        try do
          Agent.stop(disco)
        catch
          _, _ -> :ok
        end
      end)

      %{disco: disco}
    end

    defp set_disco(nodes), do: Agent.update(:test_disco, fn _ -> nodes end)
    defp get_disco, do: Agent.get(:test_disco, & &1)

    test "init resolves nodes from MFA", %{node1: node1, node2: node2} do
      set_disco([node1, node2])

      # Ensure nodes are connected so connect_all inside activate_source
      # can reach them and Manager.node_up succeeds.
      Node.connect(node1)
      Node.connect(node2)

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 60_000
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes after MFA reconfigure (init test)"

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 10_000),
             "node2 not in Manager.live_nodes after MFA reconfigure (init test)"
    end

    test "refresh adds a node that appears in the MFA result", %{node1: node1, node2: node2} do
      set_disco([node1])

      # Ensure node1 is connected so the initial MFA resolve can add it.
      Node.connect(node1)

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 200
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes after MFA reconfigure (refresh adds test)"

      refute node2 in Manager.live_nodes()

      set_disco([node1, node2])
      Node.connect(node2)

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 5_000),
             "node2 was not added after it appeared in MFA refresh"
    end

    test "refresh removes a node that disappears from the MFA result", %{
      node1: node1,
      node2: node2
    } do
      set_disco([node1, node2])

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 200
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes after MFA reconfigure (refresh removes test)"

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 10_000),
             "node2 not in Manager.live_nodes after MFA reconfigure (refresh removes test)"

      set_disco([node1])

      assert wait_until(fn -> node2 not in Manager.live_nodes() end, 3_000),
             "node2 was not removed after it disappeared from MFA refresh"

      assert node1 in Manager.live_nodes()

      set_disco([node1, node2])
      restart_node_monitor(nodes: [node1, node2])
    end

    test "MFA raising an error returns empty list — no crash", %{node1: node1, node2: node2} do
      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_mfa_raise, []},
        refresh_ms: 200
      )

      Process.sleep(600)
      assert Process.alive?(Process.whereis(NodeMonitor))

      restart_node_monitor(nodes: [node1, node2])
    end

    test "no refresh timer is scheduled for :nodes static source", %{node1: node1} do
      restart_node_monitor(nodes: [node1])

      pid = Process.whereis(NodeMonitor)
      Process.sleep(400)

      {:messages, msgs} = Process.info(pid, :messages)

      refute Enum.any?(msgs, fn
               {:refresh, _} -> true
               :refresh -> true
               _ -> false
             end),
             "Static :nodes source must not schedule :refresh ticks"
    end

    @doc false
    def test_get_disco, do: get_disco()

    @doc false
    def test_mfa_raise, do: raise("simulated discovery failure")
  end
end
