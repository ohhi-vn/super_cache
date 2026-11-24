defmodule SuperCache.Cluster.Raft.GroupTest do
  use ExUnit.Case, async: false

  alias SuperCache.Bootstrap
  alias SuperCache.Cluster.Raft.{Applied, Group}
  alias SuperCache.Cluster.Raft.Config, as: RaftConfig
  alias SuperCache.Cluster.Raft.Group.Reconciler
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  test "member derivation is deterministic for equivalent partition maps" do
    first = Group.members_from({:a@host, [:b@host, :c@host]}, 3)
    second = Group.members_from({:a@host, [:c@host, :b@host]}, 3)

    assert first == second

    assert first == [
             {Group.name(3), :a@host},
             {Group.name(3), :b@host},
             {Group.name(3), :c@host}
           ]
  end

  test "membership diff reports joins and leaves" do
    current = [{:g, :a}, {:g, :b}]
    desired = [{:g, :b}, {:g, :c}]

    assert Group.diff_members(current, desired) == {[{:g, :c}], [{:g, :a}]}
  end

  test "reconciliation of an unchanged single-node group is a no-op" do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir = Path.join(System.tmp_dir!(), "sc_group_test_#{System.unique_integer([:positive])}")
    previous = Application.get_env(:super_cache, :raft)
    Application.put_env(:super_cache, :raft, data_dir: data_dir)
    RaftConfig.reset()

    Applied.reset(0)
    Applied.reset(1)

    :ok =
      Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        num_partition: 2,
        cluster: :distributed,
        replication_mode: :raft
      )

    on_exit(fn ->
      if RaftSystem.running?(), do: RaftSystem.stop()
      if SuperCache.started?(), do: Bootstrap.stop()

      case previous do
        nil -> Application.delete_env(:super_cache, :raft)
        value -> Application.put_env(:super_cache, :raft, value)
      end

      RaftConfig.reset()
      File.rm_rf!(data_dir)
    end)

    assert :ok = RaftSystem.start!()
    assert :ok = Group.start(0)
    assert :ok = Reconciler.reconcile(0)
  end
end
