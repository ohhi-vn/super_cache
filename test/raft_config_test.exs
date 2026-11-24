defmodule SuperCache.Cluster.Raft.ConfigTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.Raft.Config

  setup do
    original = Application.get_env(:super_cache, :raft)

    on_exit(fn ->
      case original do
        nil -> Application.delete_env(:super_cache, :raft)
        value -> Application.put_env(:super_cache, :raft, value)
      end

      Config.reset()
    end)

    Config.reset()
    :ok
  end

  test "defaults are used when no application config is present" do
    Application.delete_env(:super_cache, :raft)
    assert Config.get(:command_timeout_ms) == 2_000
    assert Config.get(:wal_sync_method) == :datasync
    assert Config.get(:wal_max_size_bytes) == 256_000_000
    assert is_binary(Config.get(:data_dir))
    assert :ok = Config.validate!()
  end

  test "application config overrides defaults" do
    Application.put_env(:super_cache, :raft,
      data_dir: "/tmp/super_cache_raft_test",
      command_timeout_ms: 5_000,
      wal_sync_method: :fsync,
      wal_max_size_bytes: 1_000
    )

    assert Config.get(:data_dir) == "/tmp/super_cache_raft_test"
    assert Config.get(:command_timeout_ms) == 5_000
    assert Config.get(:wal_sync_method) == :fsync
    assert :ok = Config.validate!()
  end

  test "rejects an invalid timeout" do
    Application.put_env(:super_cache, :raft, command_timeout_ms: 0)
    assert_raise ArgumentError, ~r/command_timeout_ms/, fn -> Config.validate!() end
  end

  test "rejects an invalid WAL sync method" do
    Application.put_env(:super_cache, :raft, wal_sync_method: :bogus)
    assert_raise ArgumentError, ~r/wal_sync_method/, fn -> Config.validate!() end
  end

  test "rejects a non-binary data dir" do
    Application.put_env(:super_cache, :raft, data_dir: 123)
    assert_raise ArgumentError, ~r/data_dir/, fn -> Config.validate!() end
  end
end
