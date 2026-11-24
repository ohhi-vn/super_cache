defmodule SuperCache.Cluster.RaftBootstrapTest do
  use ExUnit.Case, async: false

  alias SuperCache.Bootstrap
  alias SuperCache.Cluster.Raft.{Config, Group}
  alias SuperCache.Cluster.Raft.System, as: RaftSystem

  setup do
    if RaftSystem.running?(), do: RaftSystem.stop()
    if SuperCache.started?(), do: Bootstrap.stop()

    data_dir =
      Path.join(System.tmp_dir!(), "sc_bootstrap_test_#{System.unique_integer([:positive])}")

    previous = Application.get_env(:super_cache, :raft)
    Application.put_env(:super_cache, :raft, data_dir: data_dir)
    Config.reset()

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

  test "bootstrap starts Raft groups and stop releases them" do
    refute RaftSystem.running?()

    assert :ok =
             Bootstrap.start!(
               key_pos: 0,
               partition_pos: 0,
               num_partition: 2,
               cluster: :distributed,
               replication_mode: :raft
             )

    assert RaftSystem.running?()
    assert Process.whereis(Group.name(0)) != nil
    assert SuperCache.Config.get_config(:replication_mode) == :raft

    assert :ok = Bootstrap.stop()
    refute RaftSystem.running?()
    assert Process.whereis(Group.name(0)) == nil
  end
end
