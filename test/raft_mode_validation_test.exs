defmodule SuperCache.RaftModeValidationTest do
  @moduledoc """
  Validation coverage for the opt-in `:raft` replication mode. The full
  Raft-backed write path is exercised by the cluster tests; this suite checks
  that the mode is accepted by both bootstrap entry points while unknown
  modes are still rejected.
  """

  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Config}
  alias SuperCache.Cluster.Bootstrap, as: ClusterBootstrap

  setup do
    if SuperCache.started?(), do: Bootstrap.stop()
    Process.sleep(30)

    on_exit(fn ->
      if SuperCache.started?(), do: Bootstrap.stop()
      Config.set_config(:replication_mode, :async)
    end)

    :ok
  end

  test "local bootstrap accepts :raft as a replication_mode" do
    assert :ok =
             Bootstrap.start!(
               key_pos: 0,
               partition_pos: 0,
               num_partition: 2,
               cluster: :local,
               replication_mode: :raft
             )

    assert Config.get_config(:replication_mode) == :raft
  end

  test "cluster bootstrap still rejects unknown replication modes" do
    assert_raise ArgumentError, ~r/invalid :replication_mode/, fn ->
      ClusterBootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        num_partition: 2,
        replication_mode: :bogus
      )
    end
  end

  test "local bootstrap still rejects unknown replication modes" do
    assert_raise ArgumentError, ~r/unsupported replication_mode/, fn ->
      Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        replication_mode: :bogus
      )
    end
  end
end
