defmodule SuperCache.Cluster.RaftSubmitHardeningTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.Manager
  alias SuperCache.Cluster.Raft
  alias SuperCache.Cluster.Raft.Group
  alias SuperCache.Cluster.Raft.System, as: RaftSystem
  alias SuperCache.Test.RaftAdapterFake

  @pt_map {Manager, :partition_map}
  @pt_groups {Group, :groups}
  @pt_started {RaftSystem, :started}

  setup do
    previous_adapter = Application.get_env(:super_cache, :raft_adapter)
    Application.put_env(:super_cache, :raft_adapter, RaftAdapterFake)
    :ok = RaftAdapterFake.install()

    old_map = :persistent_term.get(@pt_map, %{})
    old_groups = :persistent_term.get(@pt_groups, [])
    old_started = :persistent_term.get(@pt_started, false)

    :persistent_term.put(@pt_map, %{0 => {node(), []}})
    :persistent_term.put(@pt_groups, [0])
    :persistent_term.put(@pt_started, true)

    on_exit(fn ->
      RaftAdapterFake.uninstall()

      case previous_adapter do
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
    end)

    :ok
  end

  test "backpressure is normalized and not retried" do
    :ok = RaftAdapterFake.install(process_command: [{:error, :backpressure}])

    assert {:error, %SuperCache.Cluster.Error{reason: :backpressure, mode: :raft}} =
             Raft.submit(0, {:put, {:k, 1}})

    assert [{:process_command, _}] = RaftAdapterFake.trace()
  end

  test "durable-write failure is normalized and not retried" do
    :ok = RaftAdapterFake.install(process_command: [{:error, :wal_down}])

    assert {:error, %SuperCache.Cluster.Error{reason: :wal_down, mode: :raft}} =
             Raft.submit(0, {:put, {:k, 1}})

    assert [{:process_command, _}] = RaftAdapterFake.trace()
  end

  test "not_appended tuple is unwrapped to its reason" do
    :ok =
      RaftAdapterFake.install(
        process_command: [{:error, {:not_appended, :cluster_change_not_permitted, %{}, []}}]
      )

    assert {:error, %SuperCache.Cluster.Error{reason: :cluster_change_not_permitted}} =
             Raft.submit(0, {:put, {:k, 1}})
  end

  test "reconcile refuses catch-up reformation when committed data exists" do
    alias SuperCache.Cluster.Raft.Group.Reconciler

    leader = {Group.name(0), :leader_node}
    register_local_server()

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}],
        key_metrics: fn
          ^leader, _timeout -> %{commit_index: 5}
          _server, _timeout -> %{commit_index: 7}
        end,
        promote_member: [{:error, :catch_up_timeout}]
      )

    assert {:error, :committed_data_present} = Reconciler.reconcile(0)
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:restart_server, _}, &1))
  end

  test "reconcile allows catch-up reformation when no committed data exists" do
    alias SuperCache.Cluster.Raft.Group.Reconciler

    leader = {Group.name(0), :leader_node}
    register_local_server()

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:ok, leader}],
        members: [{:ok, [], leader}],
        key_metrics: %{},
        promote_member: [{:error, :catch_up_timeout}],
        restart_server: [{:ok, self()}]
      )

    assert :ok = Reconciler.reconcile(0)
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:restart_server, _}, &1))
  end

  test "reconcile allows first formation when no committed data exists" do
    alias SuperCache.Cluster.Raft.Group.Reconciler

    :ok =
      RaftAdapterFake.install(
        find_leader: [{:error, :no_leader}],
        key_metrics: %{},
        restart_server: [{:ok, self()}]
      )

    assert :ok = Reconciler.reconcile(0)
    assert Enum.any?(RaftAdapterFake.trace(), &match?({:restart_server, _}, &1))
  end

  defp register_local_server do
    name = Group.name(0)

    if Process.whereis(name) do
      Process.unregister(name)
    end

    Process.register(self(), name)
    on_exit(fn -> if Process.whereis(name) == self(), do: Process.unregister(name) end)
  end
end
