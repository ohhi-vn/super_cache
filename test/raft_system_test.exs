defmodule SuperCache.Cluster.Raft.SystemTest do
  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Config, Partition, Storage}
  alias SuperCache.Cluster.Raft.{Applied, Config, Group}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "super_cache_raft_system_test_#{System.unique_integer([:positive])}"
      )

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

    %{data_dir: data_dir}
  end

  test "starts and stops a system with a local group", %{data_dir: data_dir} do
    assert RaftSystem.running?()
    assert :ok = RaftSystem.start!()
    assert RaftSystem.running?()
    assert RaftEx.App.System.fetch(RaftSystem.system_name()) != nil

    assert :ok = Group.start(0)
    assert Process.whereis(Group.name(0)) != nil

    assert {:ok, :ok, _leader} =
             RaftEx.process_command(Group.server_id(0), {:put, {:system_key, "v"}}, 5_000)

    assert Storage.get(:system_key, Partition.get_partition_by_idx(0)) == [{:system_key, "v"}]

    assert :ok = RaftSystem.stop()
    refute RaftSystem.running?()
    assert Process.whereis(Group.name(0)) == nil
    assert RaftEx.App.System.fetch(RaftSystem.system_name()) == nil
    assert File.exists?(data_dir)
  end
end
