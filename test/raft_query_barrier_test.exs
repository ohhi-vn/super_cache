defmodule SuperCache.Cluster.RaftQueryBarrierTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Metrics, Router}
  alias SuperCache.Cluster.Raft.{Query, System, Topology}
  alias SuperCache.{Partition, Storage}
  alias SuperCache.Test.RaftAdapterFake

  @started_key {System, :started}

  setup do
    if SuperCache.started?(), do: SuperCache.stop()

    previous_adapter = Application.get_env(:super_cache, :raft_adapter)
    previous_mode = SuperCache.Config.get_config(:replication_mode, :async)
    previous_topology = :persistent_term.get({Topology, :status}, nil)

    Application.put_env(:super_cache, :raft_adapter, RaftAdapterFake)
    :ok = RaftAdapterFake.install()

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 1, cluster: :distributed)
    SuperCache.Config.set_config(:replication_mode, :raft)
    :persistent_term.put(@started_key, true)

    key = {:raft_query_key, 1}
    Storage.put(key, Partition.get_partition(key))

    on_exit(fn ->
      :persistent_term.erase(@started_key)

      case previous_topology do
        nil -> :persistent_term.erase({Topology, :status})
        value -> :persistent_term.put({Topology, :status}, value)
      end

      SuperCache.Config.set_config(:replication_mode, previous_mode)
      SuperCache.stop()

      case previous_adapter do
        nil -> Application.delete_env(:super_cache, :raft_adapter)
        value -> Application.put_env(:super_cache, :raft_adapter, value)
      end

      RaftAdapterFake.uninstall()
    end)

    %{key: key}
  end

  test "local reads pass through the local query barrier", %{key: key} do
    assert [^key] = Router.route_get!(key, read_mode: :local)

    assert Enum.any?(RaftAdapterFake.trace(), &match?({:local_query, _}, &1))
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:leader_query, _}, &1))
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:consistent_query, _}, &1))
  end

  test "primary reads pass through the leader query barrier", %{key: key} do
    assert [^key] = Router.route_get!(key, read_mode: :primary)

    assert Enum.any?(RaftAdapterFake.trace(), &match?({:leader_query, _}, &1))
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:consistent_query, _}, &1))
  end

  test "consistent reads pass through the quorum-confirmed barrier", %{key: key} do
    assert [^key] = Router.route_get!(key, read_mode: :consistent)

    assert Enum.any?(RaftAdapterFake.trace(), &match?({:consistent_query, _}, &1))
  end

  test "read result shapes are unchanged across modes", %{key: key} do
    expected = [key]

    for mode <- [:local, :primary, :consistent] do
      assert Router.route_get!(key, read_mode: mode) == expected
    end
  end

  test "barrier is scoped to a single call", %{key: key} do
    assert Router.route_get!(key, read_mode: :consistent) == [key]
    :ok = RaftAdapterFake.install()

    assert Router.route_get!(key, read_mode: :local) == [key]
    refute Enum.any?(RaftAdapterFake.trace(), &match?({:consistent_query, _}, &1))
  end

  test "unavailable quorum fails the consistent read and counts a Raft failure", %{key: key} do
    :ok = RaftAdapterFake.install(consistent_query: [{:error, :no_leader}])
    before = Map.get(Metrics.get_all(:replication), :raft_failures, 0)

    assert_raise SuperCache.Cluster.Error, ~r/no_leader/, fn ->
      Router.route_get!(key, read_mode: :consistent)
    end

    assert {:error, %SuperCache.Cluster.Error{reason: reason}} =
             SuperCache.get(key, read_mode: :consistent)

    assert reason == :no_leader
    assert Map.get(Metrics.get_all(:replication), :raft_failures, 0) > before
  end

  test "barrier uses the configured MFA and is a no-op without Raft" do
    assert {:ok, :state} = Query.confirm(:state)

    :persistent_term.erase(@started_key)
    assert :ok = Query.barrier(:consistent, 0)
  end
end
