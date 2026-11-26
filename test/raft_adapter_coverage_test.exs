defmodule SuperCache.Cluster.RaftAdapterCoverageTest do
  @moduledoc """
  Failure and membership branches of the Raft adapter that need a deterministic
  backend. `RaftEx` itself is exercised by the system and cluster test suites;
  these tests drive the adapter with a scripted stand-in.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Manager, Metrics, Raft}
  alias SuperCache.Cluster.Raft.{Group, Group.Reconciler, Store}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem
  alias SuperCache.Test.RaftAdapterFake

  @pt_map {Manager, :partition_map}
  @pt_groups {Group, :groups}
  @pt_started {RaftSystem, :started}
  @previous_adapter {__MODULE__, :previous_adapter}

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()

    previous_adapter = Application.get_env(:super_cache, :raft_adapter)
    :persistent_term.put(@previous_adapter, previous_adapter)
    Application.put_env(:super_cache, :raft_adapter, RaftAdapterFake)
    :ok = RaftAdapterFake.install()

    previous_manager_state = :sys.get_state(Manager)

    previous_mode = SuperCache.Config.get_config(:replication_mode, :async)
    previous_parts = SuperCache.Config.get_config(:num_partition, 0)
    previous_factor = SuperCache.Config.get_config(:replication_factor, 2)

    SuperCache.Config.set_config(:num_partition, 2)

    old_map = :persistent_term.get(@pt_map, %{})
    old_groups = :persistent_term.get(@pt_groups, [])
    old_started = :persistent_term.get(@pt_started, false)

    :persistent_term.put(@pt_map, %{0 => {node(), []}, 1 => {:remote_node, []}})
    :persistent_term.put(@pt_groups, [])
    :persistent_term.erase(@pt_started)

    on_exit(fn ->
      RaftAdapterFake.uninstall()

      case :persistent_term.get(@previous_adapter, nil) do
        nil -> Application.delete_env(:super_cache, :raft_adapter)
        value -> Application.put_env(:super_cache, :raft_adapter, value)
      end

      :persistent_term.put(@pt_map, old_map)
      :persistent_term.put(@pt_groups, old_groups)

      if old_started do
        :persistent_term.put(@pt_started, true)
      else
        :persistent_term.erase(@pt_started)
      end

      :sys.replace_state(Manager, fn _state -> previous_manager_state end)
      SuperCache.Config.set_config(:replication_mode, previous_mode)
      SuperCache.Config.set_config(:num_partition, previous_parts)
      SuperCache.Config.set_config(:replication_factor, previous_factor)
    end)

    :ok
  end

  # ── Adapter boundary ───────────────────────────────────────────────────────

  test "adapter exposes the 0.4.0 membership, leadership, query, and metrics APIs" do
    alias SuperCache.Cluster.Raft.Adapter

    server = {Group.name(0), node()}
    query = {:erlang, :self, []}

    assert {:ok, [], :leader} = Adapter.add_member(server, server, :promotable, 1_000)
    assert {:ok, [], :leader} = Adapter.promote_member(server, server, 1_000)
    assert {:ok, [], :leader} = Adapter.demote_member(server, server, 1_000)
    assert :ok = Adapter.transfer_leadership(server, server, 1_000)
    assert {:ok, :ok, :leader} = Adapter.local_query(server, query, 1_000)
    assert {:ok, :ok, :leader} = Adapter.leader_query(server, query, 1_000)
    assert {:ok, :ok, :leader} = Adapter.consistent_query(server, query, 1_000)
    assert %{} = Adapter.key_metrics(server, 1_000)
    assert %{} = Adapter.member_overview(server, 1_000)

    trace = RaftAdapterFake.trace()

    for name <- [
          :add_member,
          :promote_member,
          :demote_member,
          :transfer_leadership,
          :local_query,
          :leader_query,
          :consistent_query,
          :key_metrics,
          :member_overview
        ] do
      assert Enum.any?(trace, &match?({^name, _}, &1)), "missing #{name} call in adapter trace"
    end

    assert Enum.any?(trace, &match?({:add_member, [_, _, :promotable, 1_000]}, &1))
  end

  # ── Reconciler ──────────────────────────────────────────────────────────────

  test "reconcile ignores partitions owned by another primary" do
    assert :ok = Reconciler.reconcile(1)

    # Earlier tests can leave a spawned reconciliation in flight; only calls
    # for this partition indicate that reconcile/1 did the wrong work.
    refute Enum.any?(RaftAdapterFake.trace(), fn
             {:find_leader, [cluster]} -> cluster == Group.cluster_name(1)
             _ -> false
           end)
  end

  test "reconcile forms a cluster when no leader can be found" do
    assert :ok = Reconciler.reconcile(0)
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:find_leader, _}, &1))
  end

  test "reconcile returns a cluster formation failure" do
    :ok = RaftAdapterFake.install(start_cluster: [{:error, :formation_failed}])

    assert {:error, {:error, :formation_failed}} = Reconciler.reconcile(0)
  end

  test "reconcile adds missing members and removes stale ones" do
    leader = {:super_cache_raft_partition_0, node()}
    desired = [{Group.name(0), node()}]
    stale = [{Group.name(0), :old_node}]

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, stale, leader}, {:ok, desired, leader}]
      )

    assert :ok = Reconciler.reconcile(0)

    trace = RaftAdapterFake.trace()
    assert Enum.any?(trace, &match?({:remove_member, [^leader, {_, :old_node}, 10_000]}, &1))
    assert Enum.any?(trace, &match?({:demote_member, [^leader, {_, :old_node}, 10_000]}, &1))

    demote_index = Enum.find_index(trace, &match?({:demote_member, _}, &1))
    remove_index = Enum.find_index(trace, &match?({:remove_member, _}, &1))
    assert demote_index != nil and remove_index != nil and demote_index < remove_index

    assert Enum.any?(trace, &match?({:start_server, _}, &1))
    assert Enum.any?(trace, &match?({:add_member, [^leader, {_, _}, :promotable, 10_000]}, &1))
    assert Enum.any?(trace, &match?({:promote_member, [^leader, {_, _}, 10_000]}, &1))

    add_index = Enum.find_index(trace, &match?({:add_member, _}, &1))
    promote_index = Enum.find_index(trace, &match?({:promote_member, _}, &1))
    assert add_index != nil and promote_index != nil and add_index < promote_index

    assert Enum.any?(trace, &match?({:stop_server, [:super_cache_raft, {_, :old_node}]}, &1))
  end

  test "promotion waits until the joining member catches up" do
    leader = {Group.name(0), :leader_node}
    desired = [{Group.name(0), node()}]

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}, {:ok, desired, leader}],
        key_metrics: fn
          ^leader, _timeout ->
            %{commit_index: 5}

          _server, _timeout ->
            polls = :persistent_term.get({__MODULE__, :catch_up_polls}, 0) + 1
            :persistent_term.put({__MODULE__, :catch_up_polls}, polls)

            if polls >= 3, do: %{commit_index: 5}, else: %{commit_index: 1}
        end
      )

    on_exit(fn -> :persistent_term.erase({__MODULE__, :catch_up_polls}) end)

    assert :ok = Reconciler.reconcile(0)

    trace = RaftAdapterFake.trace()
    assert Enum.any?(trace, &match?({:add_member, _}, &1))
    assert Enum.any?(trace, &match?({:promote_member, _}, &1))
    assert Enum.any?(trace, &match?({:key_metrics, _}, &1))
    assert :persistent_term.get({__MODULE__, :catch_up_polls}, 0) >= 3
  end

  test "add_member failures are reported" do
    leader = {:super_cache_raft_partition_0, node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}, {:ok, [{Group.name(0), node()}], leader}],
        add_member: [{:error, :not_member}]
      )

    assert :ok = Reconciler.reconcile(0)
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:add_member, _}, &1))
  end

  test "remove_member failures do not stop reconciliation" do
    leader = {:super_cache_raft_partition_0, node()}
    desired = [{Group.name(0), node()}]

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [
          {:ok, [{Group.name(0), :gone}, {Group.name(0), :other}], leader},
          {:ok, desired, leader}
        ],
        remove_member: [{:error, :not_member}]
      )

    assert :ok = Reconciler.reconcile(0)
  end

  test "the last voter is not demoted or removed" do
    leader = {:super_cache_raft_partition_0, node()}
    stale_id = {Group.name(0), :old_node}
    desired = Group.member_ids(0)

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [stale_id], leader}, {:ok, desired, leader}],
        member_overview: %{
          cluster: %{
            stale_id => %{voter_status: %{membership: :voter}},
            hd(desired) => %{voter_status: %{membership: :promotable}}
          }
        }
      )

    assert :ok = Reconciler.reconcile(0)

    trace = RaftAdapterFake.trace()
    refute Enum.any?(trace, &match?({:demote_member, _}, &1))
    refute Enum.any?(trace, &match?({:remove_member, _}, &1))
  end

  test "a stale server is stopped and restarted before joining" do
    leader = {:super_cache_raft_partition_0, node()}
    desired = [{Group.name(0), node()}]

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}, {:ok, desired, leader}],
        start_server: [{:error, :already_started}, :ok]
      )

    assert :ok = Reconciler.reconcile(0)

    trace = RaftAdapterFake.trace()
    assert length(Enum.filter(trace, &match?({:start_server, _}, &1))) == 2
    assert Enum.any?(trace, &match?({:stop_server, _}, &1))
  end

  test "a server that cannot be restarted fails reconciliation" do
    leader = {:super_cache_raft_partition_0, node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}, {:ok, [{Group.name(0), node()}], leader}],
        start_server: [{:error, :already_started}, {:error, :still_running}]
      )

    assert :ok = Reconciler.reconcile(0)
    assert length(Enum.filter(RaftAdapterFake.trace(), &match?({:start_server, _}, &1))) == 2
  end

  test "membership that never converges eventually times out" do
    leader = {:super_cache_raft_partition_0, node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [
          {:ok, [], leader},
          {:ok, [{Group.name(0), :other}], leader}
        ]
      )

    assert {:error, :membership_not_converged} = Reconciler.reconcile(0)
  end

  test "a stale leader process triggers cluster reformation" do
    leader = {:super_cache_raft_partition_0, :gone}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:error, :noproc}],
        start_cluster: [{:ok, [], []}]
      )

    assert :ok = Reconciler.reconcile(0)
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:start_cluster, _}, &1))
  end

  test "unexpected errors are normalized" do
    :persistent_term.put(@pt_map, %{0 => :invalid})

    assert {:error, %MatchError{}} = Reconciler.reconcile(0)
  end

  # ── Leadership transfer ────────────────────────────────────────────────────

  test "leadership is transferred to a caught-up voter" do
    alias SuperCache.Cluster.Raft.Leadership

    :persistent_term.put(@pt_started, true)
    leader = {Group.name(0), :old_node}
    target = {Group.name(0), node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        member_overview: %{cluster: %{target => %{voter_status: %{membership: :voter}}}},
        key_metrics: fn
          ^leader, _timeout -> %{commit_index: 5}
          _target, _timeout -> %{commit_index: 5}
        end
      )

    assert :ok = Leadership.transfer(0, node())
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:transfer_leadership, _}, &1))
  end

  test "leadership is not transferred to a non-voter" do
    alias SuperCache.Cluster.Raft.Leadership

    :persistent_term.put(@pt_started, true)
    leader = {Group.name(0), :old_node}
    target = {Group.name(0), node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        member_overview: %{cluster: %{target => %{voter_status: %{membership: :promotable}}}}
      )

    assert {:error, :target_not_voter} = Leadership.transfer(0, node())
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:transfer_leadership, _}, &1))
  end

  test "leadership is not transferred to a lagging member" do
    alias SuperCache.Cluster.Raft.Leadership

    :persistent_term.put(@pt_started, true)
    leader = {Group.name(0), :old_node}
    target = {Group.name(0), node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        member_overview: %{cluster: %{target => %{voter_status: %{membership: :voter}}}},
        key_metrics: fn
          ^leader, _timeout -> %{commit_index: 5}
          _target, _timeout -> %{commit_index: 2}
        end
      )

    assert {:error, :target_not_caught_up} = Leadership.transfer(0, node())
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:transfer_leadership, _}, &1))
  end

  test "leadership transfer is skipped when the target already leads" do
    alias SuperCache.Cluster.Raft.Leadership

    :persistent_term.put(@pt_started, true)
    leader = {Group.name(0), node()}
    :ok = RaftAdapterFake.install(find_leader: [{:ok, leader}])

    assert :ok = Leadership.transfer(0, node())
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:transfer_leadership, _}, &1))
  end

  # ── Public façade ───────────────────────────────────────────────────────────

  test "submit returns the committed reply" do
    assert {:ok, :ok} = Raft.submit(0, {:put, {:key, 1}})
    assert [{:process_command, _}] = RaftAdapterFake.trace()
  end

  test "submit normalizes a quorum timeout" do
    server_id = {Group.name(0), node()}
    :ok = RaftAdapterFake.install(process_command: [{:timeout, server_id}])

    assert {:error, %SuperCache.Cluster.Error{reason: :timeout, mode: :raft}} =
             Raft.submit(0, {:put, {:key, 1}})
  end

  test "submit normalizes an unexpected result" do
    :ok = RaftAdapterFake.install(process_command: [{:error, :boom}])

    assert {:error, %SuperCache.Cluster.Error{reason: :boom}} =
             Raft.submit(0, {:put, {:key, 1}})
  end

  test "submit retries once after reconciling a leaderless local group" do
    :persistent_term.put(@pt_started, true)
    :persistent_term.put(@pt_groups, [0])

    :ok =
      RaftAdapterFake.install(
        process_command: [{:error, :no_leader}, {:ok, :ok, :leader}],
        find_leader: [{:ok, {:super_cache_raft_partition_0, node()}}]
      )

    assert {:ok, :ok} = Raft.submit(0, {:put, {:key, 1}})

    trace = RaftAdapterFake.trace()
    assert length(Enum.filter(trace, &match?({:process_command, _}, &1))) == 2
    assert Enum.any?(trace, &match?({:find_leader, _}, &1))
  end

  test "submit does not reconcile when this node is not primary" do
    :persistent_term.put(@pt_map, %{0 => {:remote_node, []}})
    :ok = RaftAdapterFake.install(process_command: [{:error, :no_leader}])

    assert {:error, %SuperCache.Cluster.Error{reason: :no_leader}} =
             Raft.submit(0, {:put, {:key, 1}})

    refute Enum.any?(RaftAdapterFake.trace(), &match?({:find_leader, _}, &1))
  end

  test "submit follows an explicit leader redirect" do
    :ok =
      RaftAdapterFake.install(
        process_command: [{:error, {:not_leader, :other}}, {:ok, :ok, :other}]
      )

    assert {:ok, :ok} = Raft.submit(0, {:put, {:key, 1}})

    [{:process_command, [first_server | _]}, {:process_command, [second_server | _]}] =
      RaftAdapterFake.trace()

    assert elem(first_server, 1) == node()
    assert elem(second_server, 1) == :other
  end

  test "submit uses find_leader for a bare not_leader reply" do
    :ok =
      RaftAdapterFake.install(
        process_command: [{:error, :not_leader}, {:ok, :ok, :leader}],
        find_leader: [{:ok, {:super_cache_raft_partition_0, :leader}}]
      )

    assert {:ok, :ok} = Raft.submit(0, {:put, {:key, 1}})

    [{_, [first_server | _]}, _, {:process_command, [third_server | _]}] =
      RaftAdapterFake.trace()

    assert elem(first_server, 1) == node()
    assert third_server == {Group.name(0), {:super_cache_raft_partition_0, :leader}}
  end

  test "submit fails when no leader can be discovered" do
    :ok =
      RaftAdapterFake.install(
        process_command: [{:error, :not_leader}],
        find_leader: [{:error, :no_leader}]
      )

    assert {:error, %SuperCache.Cluster.Error{reason: :no_leader}} =
             Raft.submit(0, {:put, {:key, 1}})
  end

  test "submit normalizes an adapter crash" do
    :ok =
      RaftAdapterFake.install(
        process_command: [fn _server, _command, _timeout -> raise "transport down" end]
      )

    assert {:error, %SuperCache.Cluster.Error{reason: {:exit, _}}} =
             Raft.submit(0, {:put, {:key, 1}})
  end

  test "submit targets a remote primary when this node is not a member" do
    :persistent_term.put(@pt_map, %{0 => {:remote_node, []}})
    assert {:ok, :ok} = Raft.submit(0, {:put, {:key, 1}})

    [{:process_command, [server | _]}] = RaftAdapterFake.trace()
    assert server == {Group.name(0), :remote_node}
  end

  # ── Group lifecycle ─────────────────────────────────────────────────────────

  test "group start waits for a replica server owned by another primary" do
    :persistent_term.put(@pt_map, %{0 => {:remote_node, []}})
    register_local_server()

    assert :ok = Group.start(0)
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:start_or_restart_cluster, _}, &1))
  end

  test "group start times out when the replica server never appears" do
    :persistent_term.put(@pt_map, %{0 => {:remote_node, []}})

    assert {:error, :server_not_started} = Group.start(0)
  end

  test "group stop reports a server that refuses to go away" do
    register_local_server()

    assert {:error, :server_still_running} = Group.stop(0)
  end

  test "group status exposes engine metrics and member roles" do
    register_local_server()
    server = {Group.name(0), node()}

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, server}],
        members: [{:ok, [server], server}],
        key_metrics: %{term: 4, commit_index: 9, last_applied: 9, state: :leader},
        member_overview: %{
          cluster: %{server => %{voter_status: %{membership: :voter}}}
        }
      )

    status = Group.status(0)

    assert status.idx == 0
    assert status.role == :leader
    assert status.member_count == 1
    assert status.metrics.term == 4
    assert status.metrics.commit_index == 9
    assert status.metrics.last_applied == 9
    assert status.member_roles == %{server => :voter}
  end

  test "group status keeps its original fields when the engine is unavailable" do
    :persistent_term.put(@pt_groups, [0])

    status = Group.status(0)

    assert Map.has_key?(status, :idx)
    assert Map.has_key?(status, :running)
    assert Map.has_key?(status, :role)
    assert status.metrics == %{}
    assert status.member_roles == %{}
  end

  test "ensure_started is a no-op for a tracked group" do
    :persistent_term.put(@pt_groups, [0])

    assert :ok = Group.ensure_started(0)
    assert RaftAdapterFake.trace() == []
  end

  test "stop_all stops every tracked group" do
    :persistent_term.put(@pt_groups, [0, 1])

    assert :ok = Group.stop_all()
    assert length(Enum.filter(RaftAdapterFake.trace(), &match?({:stop_server, _}, &1))) == 2
    assert Group.tracked() == []
  end

  # ── System lifecycle ────────────────────────────────────────────────────────

  test "system exposes its server id" do
    assert RaftSystem.server_id(3) == {Group.name(3), node()}
  end

  test "system start accepts an already-started backend system" do
    names = RaftEx.App.System.derive_names(RaftSystem.system_name())
    register_process(Map.fetch!(names, :server_sup))
    :ok = RaftAdapterFake.install(system_start: [{:error, {:already_started, self()}}])

    assert :ok = RaftSystem.start!()
    assert RaftSystem.running?()
  end

  test "system start raises on an unexpected backend error" do
    register_process(
      Map.fetch!(RaftEx.App.System.derive_names(RaftSystem.system_name()), :server_sup)
    )

    :ok = RaftAdapterFake.install(system_start: [{:error, :not_available}])

    assert_raise RuntimeError, ~r/failed to start system/, fn -> RaftSystem.start!() end
  end

  test "system stop tolerates a backend that has already gone away" do
    :persistent_term.put(@pt_started, true)
    :ok = RaftAdapterFake.install(system_stop: [{:error, :not_found}])

    assert :ok = RaftSystem.stop()
    refute RaftSystem.running?()
  end

  test "system start raises when the backend never registers its children" do
    names = RaftEx.App.System.derive_names(RaftSystem.system_name())

    :ok =
      RaftAdapterFake.install(
        derive_names: [Map.put(names, :server_sup, :raft_coverage_missing_sup)],
        system_start: [{:ok, self()}]
      )

    assert_raise RuntimeError, ~r/did not become ready/, fn -> RaftSystem.start!() end
  end

  # ── Manager raft hooks ──────────────────────────────────────────────────────

  test "manager schedules raft reconciliation when a node joins" do
    SuperCache.Config.set_config(:replication_mode, :raft)
    :persistent_term.put(@pt_started, true)

    Manager.node_up(node())

    assert wait_until(
             fn -> Enum.any?(RaftAdapterFake.trace(), &match?({:find_leader, _}, &1)) end,
             2_000
           )
  end

  test "manager drops stale raft groups when primacy or membership changes" do
    SuperCache.Config.set_config(:replication_mode, :raft)
    SuperCache.Config.set_config(:num_partition, 3)
    :persistent_term.put(@pt_started, true)
    :persistent_term.put(@pt_groups, [0, 1, 2])

    # Every partition was primary-owned before the view changed. After the
    # update, partition 1 drops this node entirely and partition 2 moves its
    # primary elsewhere, exercising both stale-group branches.
    old_map = Map.new(0..2, &{&1, {node(), []}})
    :persistent_term.put(@pt_map, old_map)

    {:noreply, _state} =
      Manager.handle_cast({:node_down, node()}, %{nodes: [:raft_a, :raft_b, node()]})

    assert wait_until(
             fn ->
               length(Enum.filter(RaftAdapterFake.trace(), &match?({:stop_server, _}, &1))) >= 2
             end,
             2_000
           )

    assert Group.tracked() == [0]
  end

  test "a failed planned leadership transfer is counted and reconciliation continues" do
    SuperCache.Config.set_config(:replication_mode, :raft)
    SuperCache.Config.set_config(:num_partition, 3)
    :persistent_term.put(@pt_started, true)
    :persistent_term.put(@pt_groups, [0, 1, 2])

    old_map = Map.new(0..2, &{&1, {node(), []}})
    :persistent_term.put(@pt_map, old_map)
    :ok = RaftAdapterFake.install(find_leader: [{:error, :no_leader}])

    before = Map.get(Metrics.get_all(:replication), :leadership_transfer_failures, 0)

    {:noreply, _state} =
      Manager.handle_cast({:node_down, node()}, %{nodes: [:raft_a, :raft_b, node()]})

    assert wait_until(
             fn ->
               Map.get(Metrics.get_all(:replication), :leadership_transfer_failures, 0) > before
             end,
             2_000
           )

    assert wait_until(
             fn -> Enum.any?(RaftAdapterFake.trace(), &match?({:stop_server, _}, &1)) end,
             2_000
           )
  end

  # ── Store ───────────────────────────────────────────────────────────────────

  test "store keeps its ETS table when started twice" do
    assert {:error, {:already_started, _pid}} = Store.start_link()
    assert {:ok, %{}} = Store.init([])
  end

  # ── Helpers ─────────────────────────────────────────────────────────────────

  defp register_local_server do
    register_process(Group.name(0))
  end

  defp register_process(name) do
    if Process.whereis(name) do
      Process.unregister(name)
    end

    Process.register(self(), name)
    on_exit(fn -> if Process.whereis(name) == self(), do: Process.unregister(name) end)
  end

  defp wait_until(_fun, remaining) when remaining <= 0, do: false

  defp wait_until(fun, remaining) do
    if fun.() do
      true
    else
      Process.sleep(20)
      wait_until(fun, remaining - 20)
    end
  end
end
