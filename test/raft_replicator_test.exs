defmodule SuperCache.Cluster.RaftReplicatorTest do
  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Config, Partition, Storage}
  alias SuperCache.Cluster.{HealthMonitor, Metrics, Replicator, Stats}
  alias SuperCache.Cluster.Raft.{Applied, Config, Group}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir =
      Path.join(System.tmp_dir!(), "sc_replicator_test_#{System.unique_integer([:positive])}")

    previous = Application.get_env(:super_cache, :raft)
    Application.put_env(:super_cache, :raft, data_dir: data_dir)
    Config.reset()

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

      Config.reset()
      File.rm_rf!(data_dir)
    end)

    Applied.reset(0)
    Applied.reset(1)
    assert :ok = RaftSystem.start!()
    assert :ok = Group.start(0)
    :ok
  end

  test "single write goes through the Raft commit path" do
    assert :ok = Replicator.replicate(0, :put, {:raft_rep_key, "v"})

    assert Storage.get(:raft_rep_key, Partition.get_partition_by_idx(0)) == [
             {:raft_rep_key, "v"}
           ]

    assert Map.get(Metrics.get_all({:api, :replicate_raft}), :calls, 0) >= 1
  end

  test "batch write produces one Raft commit and one metric" do
    before = Map.get(Metrics.get_all({:api, :replicate_batch_raft}), :calls, 0)

    assert :ok =
             Replicator.replicate_batch(0, :put, [
               {:raft_batch_a, 1},
               {:raft_batch_b, 2}
             ])

    partition = Partition.get_partition_by_idx(0)
    assert Storage.get(:raft_batch_a, partition) == [{:raft_batch_a, 1}]
    assert Storage.get(:raft_batch_b, partition) == [{:raft_batch_b, 2}]

    assert Map.get(Metrics.get_all({:api, :replicate_batch_raft}), :calls, 0) == before + 1
  end

  test "a failed Raft write increments the raft failure counter" do
    assert :ok = RaftSystem.stop()
    assert {:error, %SuperCache.Cluster.Error{}} = Replicator.replicate(1, :put, {:nope, 1})
    assert Map.get(Metrics.get_all(:replication), :raft_failures, 0) >= 1
    assert Stats.reliability().raft_failures >= 1
    assert %{running: false, groups: _groups} = Stats.cluster().raft
    assert %{raft: %{running: false}} = HealthMonitor.local_node_health()
  end
end
