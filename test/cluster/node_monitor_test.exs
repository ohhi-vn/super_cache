defmodule SuperCache.Cluster.NodeMonitorTest do
  @moduledoc """
  Integration tests for `SuperCache.Cluster.NodeMonitor`:

  - Option validation (forwarded from `Bootstrap.start!/1` and from
    `NodeMonitor.start_link/1` directly)
  - Static `:nodes` list — :nodeup / :nodedown gating via `watched?/2`
  - Dynamic `:nodes_mfa` source — init, refresh-add, refresh-remove,
    MFA error resilience, no-timer guarantee for static source
  - End-to-end Bootstrap forwarding: `:nodes` passed to `start!/1`
    restricts the NodeMonitor managed set
  """

  use SuperCache.ClusterCase

  # ── Setup ─────────────────────────────────────────────────────────────────────

  setup_all do
    setup_cluster!(:monitor_peer1, :monitor_peer2)
  end

  # ── NodeMonitor — option validation ──────────────────────────────────────────

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
      SuperCache.Cluster.Bootstrap.start!(@cache_opts ++ [nodes: [node1, node2]])

      assert wait_until(
               fn -> node1 in Manager.live_nodes() and node2 in Manager.live_nodes() end,
               5_000
             ),
             "Managed peers must remain in Manager.live_nodes after Bootstrap restart"

      {plain_peer, plain_node} = start_plain_peer(:monitor_bs_plain)
      on_exit(fn -> stop_peer(plain_peer) end)

      Node.connect(plain_node)
      Process.sleep(300)

      refute plain_node in Manager.live_nodes(),
             "Unmanaged plain_node #{inspect(plain_node)} was added to Manager.live_nodes — " <>
               "Bootstrap.start!/1 should have configured NodeMonitor to filter it. " <>
               "live_nodes: #{inspect(Manager.live_nodes())}"
    end
  end

  # ── NodeMonitor — static :nodes list ─────────────────────────────────────────

  describe "NodeMonitor — static :nodes list" do
    test "non-managed :nodeup events are ignored", %{node1: node1, node2: node2} do
      {plain_peer, plain_node} = start_plain_peer(:monitor_unmanaged)
      on_exit(fn -> stop_peer(plain_peer) end)

      restart_node_monitor(nodes: [node1, node2])
      Node.connect(plain_node)
      Process.sleep(300)

      refute plain_node in Manager.live_nodes(),
             "Unmanaged node #{inspect(plain_node)} was added to Manager — " <>
               "NodeMonitor should have filtered it out."
    end

    test "non-managed :nodedown events are ignored", %{node1: node1, node2: node2} do
      {plain_peer, plain_node} = start_plain_peer(:monitor_unmanaged_down)
      on_exit(fn -> stop_peer(plain_peer) end)

      restart_node_monitor(nodes: [node1, node2])
      Node.connect(plain_node)
      Process.sleep(100)
      stop_peer(plain_peer)
      Process.sleep(300)

      live = Manager.live_nodes()
      assert node1 in live
      assert node2 in live
    end

    test ":nodeup for a managed node is forwarded to Manager",
         %{agent: agent, node1: node1, node2: node2, peer1: peer1} do
      # Configure NodeMonitor BEFORE disconnecting, so connect_all runs while
      # connections are healthy.  Disconnecting first risks a race against
      # Erlang's reconnection cooldown.
      restart_node_monitor(nodes: [node1, node2])

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 5_000),
             "Precondition: node1 must be live before disconnect"

      :peer.stop(peer1)
      # Must pass the short-name atom, not the full node atom.
      # restart_peer calls :peer.start(%{name: name, host: ~c"127.0.0.1", ...}),
      # which forms the node name by concatenating name + "@" + host.
      # Passing node1 (e.g. :"monitor_peer1@127.0.0.1") would produce
      # :"monitor_peer1@127.0.0.1@127.0.0.1" — an invalid node name.
      {new_peer1, new_node1} = restart_peer(:monitor_peer1)
      Agent.update(agent, fn s -> %{s | peer1: new_peer1, node1: new_node1} end)

      true = Node.connect(new_node1)

      assert wait_until(fn -> new_node1 in Manager.live_nodes() end, 5_000),
             ":nodeup for managed node1 was not forwarded to Manager"

      wait_cluster_stable(3)
    end

    test ":nodedown for a managed node is forwarded to Manager",
         %{agent: agent, peer2: peer2, node2: node2, node1: node1} do
      restart_node_monitor(nodes: [node1, node2])
      true = wait_until(fn -> node2 in Manager.live_nodes() end)

      stop_peer(peer2)

      assert wait_until(fn -> node2 not in Manager.live_nodes() end, 5_000),
             "Manager still lists node2 after it went down"

      {new_peer2, new_node2} = restart_peer(:monitor_peer2)
      restart_node_monitor(nodes: [node1, new_node2])
      Agent.update(agent, fn s -> %{s | peer2: new_peer2, node2: new_node2} end)
      wait_cluster_stable(3)
    end

    test "self (node()) is never in managed peers list", %{node1: node1, node2: node2} do
      # Restore the monitor to a working state before this test exits.
      # Without this on_exit, NodeMonitor is left with managed = MapSet.new([])
      # (node() is filtered out by connect_all), which fires node_down for
      # node1 and node2, poisoning the next test.
      on_exit(fn -> restart_node_monitor(nodes: [node1, node2]) end)

      restart_node_monitor(nodes: [node()])
      assert Process.whereis(NodeMonitor) != nil
    end
  end

  # ── NodeMonitor — :nodes_mfa dynamic source ───────────────────────────────────

  describe "NodeMonitor — :nodes_mfa dynamic source" do
    # An Agent simulates a service-discovery endpoint whose membership list
    # changes at runtime.  All MFA calls go through the public helpers below.
    setup %{node1: node1, node2: node2} do
      {:ok, disco} = Agent.start_link(fn -> [] end, name: :monitor_test_disco)

      on_exit(fn ->
        # Switch NodeMonitor back to a static source BEFORE stopping the disco
        # agent.  If the monitor still holds the MFA source when the agent
        # dies, the next refresh tick will exit — crashing NodeMonitor and
        # poisoning the next test's reconfigure call.
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

    defp set_disco(nodes), do: Agent.update(:monitor_test_disco, fn _ -> nodes end)
    defp get_disco,        do: Agent.get(:monitor_test_disco, & &1)

    test "init resolves nodes from MFA", %{node1: node1, node2: node2} do
      set_disco([node1, node2])

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 60_000
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes after MFA reconfigure"

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 10_000),
             "node2 not in Manager.live_nodes after MFA reconfigure"
    end

    test "refresh adds a node that appears in the MFA result", %{node1: node1, node2: node2} do
      set_disco([node1])

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 200
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes"

      refute node2 in Manager.live_nodes()

      set_disco([node1, node2])
      Node.connect(node2)

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 3_000),
             "node2 was not added after it appeared in MFA refresh"
    end

    test "refresh removes a node that disappears from the MFA result",
         %{node1: node1, node2: node2} do
      set_disco([node1, node2])

      restart_node_monitor(
        nodes_mfa: {__MODULE__, :test_get_disco, []},
        refresh_ms: 200
      )

      assert wait_until(fn -> node1 in Manager.live_nodes() end, 10_000),
             "node1 not in Manager.live_nodes"

      assert wait_until(fn -> node2 in Manager.live_nodes() end, 10_000),
             "node2 not in Manager.live_nodes"

      set_disco([node1])

      assert wait_until(fn -> node2 not in Manager.live_nodes() end, 3_000),
             "node2 was not removed after it disappeared from MFA refresh"

      assert node1 in Manager.live_nodes()

      # Restore for subsequent tests.
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
               :refresh      -> true
               _             -> false
             end),
             "Static :nodes source must not schedule :refresh ticks"
    end

    # ── Public MFA targets ────────────────────────────────────────────────────
    # Must be public (`def`) so NodeMonitor can invoke them via `apply/3`.

    @doc false
    def test_get_disco, do: get_disco()

    @doc false
    def test_mfa_raise, do: raise("simulated discovery failure")
  end
end
