defmodule SuperCache.Cluster.RaftClusterTest do
  @moduledoc """
  End-to-end coverage of the opt-in `:raft` replication mode on a real
  three-node distributed Erlang cluster. Run with `mix test.cluster`.
  """

  use ExUnit.Case, async: false

  @moduletag :cluster

  alias SuperCache.Bootstrap
  alias SuperCache.Cluster.{Manager, NodeMonitor}
  alias SuperCache.Cluster.Raft.{Config, Group}
  alias SuperCache.Cluster.Raft.Group.Reconciler
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  @cache_opts [
    key_pos: 0,
    partition_pos: 0,
    cluster: :distributed,
    replication_factor: 2,
    replication_mode: :raft,
    num_partition: 2,
    table_type: :set
  ]

  setup_all do
    if node() == :nonode@nohost do
      raise "Raft cluster tests require a distributed node. Run with: mix test.cluster"
    end

    {:ok, _} = Application.ensure_all_started(:super_cache)
    stop_local()

    dir = Path.join(System.tmp_dir!(), "sc_raft_cluster_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    local_dir = Path.join(dir, "local")
    File.mkdir_p!(local_dir)
    set_raft_dir(node(), local_dir)

    :ok = Bootstrap.start!(@cache_opts)

    suffix = System.unique_integer([:positive])
    {peer1, node1} = start_peer(:"raft_peer_one_#{suffix}", Path.join(dir, "peer1"))
    {peer2, node2} = start_peer(:"raft_peer_two_#{suffix}", Path.join(dir, "peer2"))

    true = Node.connect(node1)
    true = Node.connect(node2)
    true = :erpc.call(node1, Node, :connect, [node2], 5_000)

    assert wait_until(fn -> length(Manager.live_nodes()) == 3 end, 30_000),
           "local manager did not see 3 nodes"

    NodeMonitor.reconfigure(nodes: [node1, node2])
    :erpc.call(node1, NodeMonitor, :reconfigure, [[nodes: [node(), node2]]], 5_000)
    :erpc.call(node2, NodeMonitor, :reconfigure, [[nodes: [node(), node1]]], 5_000)

    assert wait_until(
             fn ->
               Enum.all?([node(), node1, node2], fn n ->
                 try do
                   length(:erpc.call(n, Manager, :live_nodes, [], 3_000)) == 3
                 catch
                   _, _ -> false
                 end
               end)
             end,
             30_000
           ),
           "cluster views did not converge"

    Enum.each([0, 1], &Reconciler.reconcile/1)

    ExUnit.Callbacks.on_exit(fn ->
      stop_peer(peer1)
      stop_peer(peer2)
      stop_local()
      File.rm_rf!(dir)
    end)

    %{node1: node1, node2: node2, dir: dir}
  end

  setup %{node1: n1, node2: n2} do
    ensure_local_started()

    # A previous test may have removed a peer from some manager's view.
    for n <- [node(), n1, n2], peer <- [n1, n2], peer != n do
      try do
        :erpc.call(n, Manager, :node_up, [peer], 5_000)
      catch
        _, _ -> :ok
      end
    end

    assert wait_until(
             fn ->
               Enum.all?([node(), n1, n2], fn n ->
                 try do
                   length(:erpc.call(n, Manager, :live_nodes, [], 3_000)) == 3
                 catch
                   _, _ -> false
                 end
               end)
             end,
             30_000
           ),
           "cluster views did not converge before test"

    :ok
  end

  test "leader election, committed writes and follower redirects" do
    cluster = Group.cluster_name(0)

    assert wait_until(
             fn ->
               match?({:ok, _}, RaftEx.find_leader(cluster))
             end,
             10_000
           )

    {:ok, leader} = RaftEx.find_leader(cluster)
    assert leader in Group.member_ids(0)

    # Partition 0 is owned locally; partition 1 is owned by a peer, so this
    # write also exercises the primary redirect path.
    assert true = SuperCache.put!({:raft_p0_2, "local-committed"})
    assert true = SuperCache.put!({:raft_cluster_key, "committed"})

    assert wait_until(
             fn ->
               Enum.all?(Group.member_ids(0), fn id ->
                 read_on(elem(id, 1), :raft_p0_2) == "local-committed"
               end)
             end,
             10_000
           )

    assert wait_until(
             fn ->
               Enum.all?(Group.member_ids(1), fn id ->
                 read_on(elem(id, 1), :raft_cluster_key) == "committed"
               end)
             end,
             10_000
           )

    follower = Enum.find(Group.member_ids(0), &(&1 != leader))

    assert {:ok, :ok, leader_name} =
             RaftEx.process_command(follower, {:put, {:redirect_key, 1}}, 10_000)

    assert leader_name == elem(leader, 0)
  end

  test "committed entries are available after a local restart", %{dir: dir} do
    # This node is a member of partition 0, so its local log is the one that
    # must survive the restart.
    assert true = SuperCache.put!({:raft_p0_4, "persisted"})
    assert wait_until(fn -> read_on(node(), :raft_p0_4) == "persisted" end, 10_000)

    stop_local()
    Process.sleep(300)
    set_raft_dir(node(), Path.join(dir, "local"))
    assert :ok = Bootstrap.start!(@cache_opts)

    assert wait_until(
             fn ->
               try do
                 SuperCache.get!({:raft_p0_4}) == [{:raft_p0_4, "persisted"}]
               catch
                 _, _ -> false
               end
             end,
             20_000
           )
  end

  test "writes fail when the group loses quorum", %{node1: n1} do
    # The local node and n1 are the two voting members of partition 0.
    # Stop n1's server without changing the manager's view: the group still
    # has two members, but only one can acknowledge a write.
    server_id = {Group.name(0), n1}

    start_args = [
      :super_cache_raft,
      Group.cluster_name(0),
      server_id,
      Group.machine(0),
      Group.member_ids(0)
    ]

    assert :ok =
             :erpc.call(n1, RaftEx, :stop_server, [:super_cache_raft, server_id], 10_000)

    on_exit(fn ->
      try do
        :erpc.call(n1, RaftEx, :start_server, start_args, 10_000)
      catch
        _, _ -> :ok
      end
    end)

    assert {:error, _} = SuperCache.put({:raft_p0_2, 1})

    # Recovery: the peer's server can be restarted with its persisted log.
    assert :ok = :erpc.call(n1, RaftEx, :start_server, start_args, 10_000)
    assert wait_until(fn -> SuperCache.put({:raft_p0_2, 1}) == true end, 10_000)
  end

  test "membership changes are reconciled with the partition map", %{node1: n1, node2: n2} do
    # Partition 1 is the one whose replica set changes when node2 leaves
    # (partition 0 keeps the same members with or without node2).
    before = Group.member_ids(1)
    server = {Group.name(1), n1}

    on_exit(fn ->
      for n <- [node(), n1] do
        try do
          :erpc.call(n, Manager, :node_up, [n2], 5_000)
        catch
          _, _ -> :ok
        end
      end
    end)

    # Every live node must agree on the removal before the group reconciles.
    for n <- [node(), n1] do
      :ok = :erpc.call(n, Manager, :node_down, [n2], 5_000)
    end

    # Query the group on the partition's primary: the test node no longer
    # hosts a partition-1 server, so its local leaderboard is not authoritative.
    assert wait_until(
             fn ->
               try do
                 case :erpc.call(n1, RaftEx, :members, [server, 3_000], 5_000) do
                   {:ok, current, _} ->
                     current != before and n2 not in Enum.map(current, &elem(&1, 1))

                   _ ->
                     false
                 end
               catch
                 _, _ -> false
               end
             end,
             30_000
           )
  end

  # ── helpers ──────────────────────────────────────────────────────────────────

  defp start_peer(name, data_dir) do
    cookie = :erlang.get_cookie()
    code_path = :code.get_path()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [~c"-setcookie", :erlang.atom_to_list(cookie), ~c"-connect_all", ~c"false"]
      })

    Process.sleep(1_000)
    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    set_raft_dir(node, data_dir)
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [@cache_opts])
    {peer, node}
  end

  defp set_raft_dir(target, dir) do
    File.mkdir_p!(dir)

    if target == node() do
      Application.put_env(:super_cache, :raft, data_dir: dir)
      Config.reset()
    else
      :erpc.call(target, Application, :put_env, [:super_cache, :raft, [data_dir: dir]])
      :erpc.call(target, SuperCache.Cluster.Raft.Config, :reset, [])
    end
  end

  defp stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, _ -> :ok
  end

  defp stop_local do
    if RaftSystem.running?(), do: RaftSystem.stop()

    try do
      if SuperCache.started?(), do: Bootstrap.stop()
    catch
      :exit, _ -> :ok
    end
  end

  defp ensure_local_started do
    unless SuperCache.started?() do
      :ok = Bootstrap.start!(@cache_opts)
    end

    unless RaftSystem.running?() do
      :ok = RaftSystem.start!()
    end
  end

  defp read_on(target, key) do
    case :erpc.call(target, SuperCache, :get_by_key_partition!, [key, key], 4_000) do
      [{^key, value}] -> value
      _ -> nil
    end
  catch
    _, _ -> nil
  end

  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, remaining) do
    if fun.() do
      true
    else
      Process.sleep(200)
      wait_until(fun, remaining - 200)
    end
  end
end
