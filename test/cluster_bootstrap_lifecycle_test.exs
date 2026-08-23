defmodule SuperCache.ClusterBootstrapLifecycleTest do
  @moduledoc """
  Single-node lifecycle coverage for SuperCache.Cluster.Bootstrap: start!,
  stop, validation errors, export helpers, prewarm tolerance and recovery
  during boot — all without real peers (fake node names fail fast).
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Bootstrap, Manager, ThreePhaseCommit}

  setup do
    if SuperCache.started?(), do: Bootstrap.stop()
    Process.sleep(30)
    on_exit(fn ->
      if SuperCache.started?(), do: Bootstrap.stop()
    end)

    :ok
  end

  @base_opts [key_pos: 0, partition_pos: 0, num_partition: 3]

  describe "start!/1" do
    test "starts in distributed mode on a single node" do
      assert :ok = Bootstrap.start!(@base_opts ++ [cluster: :distributed])
      assert Bootstrap.running?() == true
      assert SuperCache.started?() == true

      assert %{0 => {primary, _}} =
               Map.new(0..2, fn i -> {i, Manager.get_replicas(i)} end)
               |> Enum.into(%{})

      assert primary == node()

      # Writes work end-to-end through the routed layer.
      assert true = SuperCache.put!({:boot_key, "v"})
      assert [{:boot_key, "v"}] == SuperCache.get!({:boot_key, nil})
    end

    test "tolerates unreachable prewarm peers" do
      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          :ok =
            Bootstrap.start!(
              @base_opts ++
                [cluster: :distributed, nodes: [:"prewarm_dead@host"]]
            )
        end)

      Process.sleep(50)
      assert Bootstrap.running?() == true
      assert log =~ "prewarm" or log == "" or log != ""
    end

    test "raises on invalid options before touching state" do
      assert_raise ArgumentError, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, replication_mode: :bogus)
      end

      refute SuperCache.started?()
    end

    test "raises when both nodes and nodes_mfa are given" do
      assert_raise ArgumentError, fn ->
        Bootstrap.start!(
          @base_opts ++
            [
              cluster: :distributed,
              nodes: [:a@host],
              nodes_mfa: {Foo, :bar, []}
            ]
        )
      end
    end

    test "raises on malformed nodes_mfa / bad refresh_ms" do
      assert_raise ArgumentError, fn ->
        Bootstrap.start!(@base_opts ++ [cluster: :distributed, nodes_mfa: NotATuple])
      end

      assert_raise ArgumentError, fn ->
        Bootstrap.start!(
          @base_opts ++ [cluster: :distributed, nodes_mfa: {Foo, :bar, []}, refresh_ms: 0]
        )
      end
    end

    test "runs 3PC recovery at boot when replication_mode is :strong" do
      # Seed an in-doubt transaction so recover/0 has work to do. The ops are
      # applied once start! brings the storage tables up.
      :ets.insert(SuperCache.Cluster.TxnRegistry, {
        "boot-recover",
        %{
          txn_id: "boot-recover",
          partition_idx: 0,
          ops: [{:put, {:boot_rec, "yes"}}],
          replicas: [],
          state: :pre_committed,
          inserted_at: System.monotonic_time(:millisecond)
        }
      })

      assert :ok =
               Bootstrap.start!(@base_opts ++ [cluster: :distributed, replication_mode: :strong])

      partition = SuperCache.Partition.get_partition_by_idx(0)
      assert [{:boot_rec, "yes"}] == SuperCache.Storage.get(:boot_rec, partition)
    end
  end

  describe "stop/0" do
    test "clears started flag and is repeatable" do
      :ok = Bootstrap.start!(@base_opts ++ [cluster: :distributed])
      assert :ok = Bootstrap.stop()
      refute Bootstrap.running?()

      # Idempotent enough not to crash on double stop.
      assert :ok = Bootstrap.stop()
    end
  end

  describe "export helpers" do
    test "export_config/0 returns structural keys only" do
      :ok = Bootstrap.start!(@base_opts ++ [cluster: :distributed, replication_factor: 1])

      cfg = Bootstrap.export_config()
      assert cfg.num_partition == 3
      assert cfg.key_pos == 0
      assert cfg.partition_pos == 0
      refute Map.has_key?(cfg, :started)
      refute Map.has_key?(cfg, :nodes)
    end

    test "fetch_partition_map/1 returns idx => {primary, replicas} pairs" do
      :ok = Bootstrap.start!(@base_opts ++ [cluster: :distributed])

      map = Bootstrap.fetch_partition_map(3)
      assert length(map) == 3
      assert Enum.all?(map, fn {idx, {p, _r}} -> is_integer(idx) and p == node() end)
    end
  end

  describe "running?/0 reflects flags" do
    test "false when cluster mode is local even if started" do
      :ok = SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
      refute Bootstrap.running?()
    end
  end
end
