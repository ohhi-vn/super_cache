defmodule SuperCache.Cluster.RaftFacadeTest do
  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Config, Partition, Storage}
  alias SuperCache.Cluster.Error
  alias SuperCache.Cluster.Raft
  alias SuperCache.Cluster.Raft.{Applied, Config, Group}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir =
      Path.join(System.tmp_dir!(), "sc_facade_test_#{System.unique_integer([:positive])}")

    previous = Application.get_env(:super_cache, :raft)
    Application.put_env(:super_cache, :raft, data_dir: data_dir)
    Config.reset()

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

      Config.reset()
      File.rm_rf!(data_dir)
    end)

    :ok
  end

  test "submit commits a command and applies it to the partition" do
    assert :ok = RaftSystem.start!()
    assert :ok = Group.start(0)

    assert {:ok, :ok} = Raft.submit(0, {:put, {:facade_key, "value"}})

    assert Storage.get(:facade_key, Partition.get_partition_by_idx(0)) == [
             {:facade_key, "value"}
           ]
  end

  test "failures are normalized to a distributed error" do
    assert :ok = RaftSystem.stop()
    assert {:error, %Error{} = error} = Raft.submit(0, {:put, {:missing, :group}})
    assert error.mode == :raft
    assert error.partition == 0
    assert is_binary(Exception.message(error))
  end
end
