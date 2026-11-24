defmodule SuperCache.Cluster.RaftApiTest do
  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, KeyValue}
  alias SuperCache.Cluster.Raft.{Applied, Group}
  alias SuperCache.Cluster.Raft.Config, as: RaftConfig
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir = Path.join(System.tmp_dir!(), "sc_api_test_#{System.unique_integer([:positive])}")
    previous = Application.get_env(:super_cache, :raft)
    Application.put_env(:super_cache, :raft, data_dir: data_dir)
    RaftConfig.reset()

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

    Applied.reset(0)
    Applied.reset(1)
    assert :ok = RaftSystem.start!()
    assert :ok = Group.start(0)
    assert :ok = Group.start(1)
    :ok
  end

  test "tuple and key-value writes share the Raft commit path" do
    assert true = SuperCache.put!({:raft_api_tuple, "v"})
    assert [{:raft_api_tuple, "v"}] = SuperCache.get!({:raft_api_tuple})

    assert true = KeyValue.add("raft_api_ns", :key, "value")
    assert "value" = KeyValue.get("raft_api_ns", :key)
  end

  test "batch writes commit through the same Raft path" do
    assert :ok =
             SuperCache.put_batch!([
               {:raft_batch_a, 1},
               {:raft_batch_b, 2}
             ])

    assert [{:raft_batch_a, 1}] = SuperCache.get!({:raft_batch_a})
    assert [{:raft_batch_b, 2}] = SuperCache.get!({:raft_batch_b})
  end
end
