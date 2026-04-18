defmodule SuperCache.Cluster.DistributedHelpersTest do
  @moduledoc """
  Tests for SuperCache.Cluster.DistributedHelpers — shared helpers for
  distributed read/write operations.

  ## Test categories

  - **distributed?/0**: Cluster mode detection
  - **apply_write/3**: Write application with replication strategy
  - **has_partition?/1**: Partition ownership check
  - **route_write/4**: Write routing to primary node
  - **route_read/5**: Read routing with local/primary/quorum modes
  - **read_primary/4**: Primary node reads
  - **read_quorum/4**: Quorum consensus reads
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.DistributedHelpers
  alias SuperCache.{Config, Partition, Storage}

  # ── Test helper modules (module-level to be accessible across describe blocks) ─

  defmodule TestWriter do
    @moduledoc false

    alias SuperCache.{Storage, Partition}

    def write(key, value) do
      partition = Partition.get_partition(key)
      Storage.put({key, value}, partition)
      {:ok, {key, value}}
    end

    def write_with_apply(key, value, partition_idx) do
      alias SuperCache.Cluster.DistributedHelpers

      partition = Partition.get_partition_by_idx(partition_idx)

      DistributedHelpers.apply_write(partition_idx, partition, [
        {:put, {{:routed, key}, value}}
      ])
    end
  end

  defmodule TestReader do
    @moduledoc false

    alias SuperCache.{Storage, Partition}

    def read(key) do
      partition = Partition.get_partition(key)
      Storage.get(key, partition)
    end

    def read_value(key) do
      partition = Partition.get_partition(key)

      case Storage.get(key, partition) do
        [] -> nil
        [{_, value}] -> value
        records -> List.last(records) |> elem(1)
      end
    end
  end

  # ── Setup ────────────────────────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!(
      key_pos: 0,
      partition_pos: 0,
      cluster: :local,
      num_partition: 4
    )

    :ok
  end

  setup do
    SuperCache.delete_all()
    :ok
  end

  # ── distributed?/0 ──────────────────────────────────────────────────────────

  describe "distributed?/0" do
    test "returns false in local mode" do
      Config.set_config(:cluster, :local)
      assert DistributedHelpers.distributed?() == false
    end

    test "returns false when cluster key is not set" do
      Config.delete_config(:cluster)
      # Default is :local
      assert DistributedHelpers.distributed?() == false
    after
      Config.set_config(:cluster, :local)
    end

    test "returns true in distributed mode" do
      Config.set_config(:cluster, :distributed)
      assert DistributedHelpers.distributed?() == true
    after
      Config.set_config(:cluster, :local)
    end

    test "is consistent with Config.distributed?/0" do
      Config.set_config(:cluster, :local)
      assert DistributedHelpers.distributed?() == Config.distributed?()

      Config.set_config(:cluster, :distributed)
      assert DistributedHelpers.distributed?() == Config.distributed?()
    after
      Config.set_config(:cluster, :local)
    end
  end

  # ── apply_write/3 ───────────────────────────────────────────────────────────

  describe "apply_write/3" do
    test "applies :put operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [{:put, {:dh_test, "value"}}])

      assert result == :ok
      assert Storage.get(:dh_test, partition) == [{:dh_test, "value"}]
    end

    test "applies :delete operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      # First insert
      Storage.put({:dh_del, "value"}, partition)
      assert Storage.get(:dh_del, partition) != []

      # Then delete via apply_write
      result = DistributedHelpers.apply_write(partition_idx, partition, [{:delete, :dh_del}])

      assert result == :ok
      assert Storage.get(:dh_del, partition) == []
    end

    test "applies :delete_match operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      Storage.put({:dh_match_a, 1}, partition)
      Storage.put({:dh_match_b, 2}, partition)

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [
          {:delete_match, {:dh_match_a, :_}}
        ])

      assert result == :ok
      assert Storage.get(:dh_match_a, partition) == []
      assert Storage.get(:dh_match_b, partition) != []
    end

    test "applies :delete_all operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      Storage.put({:dh_del_all_1, "a"}, partition)
      Storage.put({:dh_del_all_2, "b"}, partition)

      result = DistributedHelpers.apply_write(partition_idx, partition, [{:delete_all, nil}])

      assert result == :ok
      assert Storage.stats(partition) |> elem(1) == 0
    end

    test "applies multiple operations in order" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      ops = [
        {:put, {:dh_multi, "first"}},
        {:delete, :dh_multi},
        {:put, {:dh_multi, "second"}}
      ]

      result = DistributedHelpers.apply_write(partition_idx, partition, ops)

      assert result == :ok
      assert Storage.get(:dh_multi, partition) == [{:dh_multi, "second"}]
    end

    test "applies operations to the correct partition" do
      for idx <- 0..3 do
        partition = Partition.get_partition_by_idx(idx)
        DistributedHelpers.apply_write(idx, partition, [{:put, {{:dh_part, idx}, idx}}])
      end

      for idx <- 0..3 do
        partition = Partition.get_partition_by_idx(idx)
        assert Storage.get({:dh_part, idx}, partition) == [{{:dh_part, idx}, idx}]
      end
    end

    test "returns :ok for empty ops list" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      result = DistributedHelpers.apply_write(partition_idx, partition, [])
      assert result == :ok
    end

    test "handles put with tuple record" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      record = {{:kv, :ns, :key}, "value"}
      result = DistributedHelpers.apply_write(partition_idx, partition, [{:put, record}])

      assert result == :ok
      assert Storage.get({:kv, :ns, :key}, partition) == [record]
    end

    test "handles put with list of records (batch)" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      records = [{:batch_a, 1}, {:batch_b, 2}]
      result = DistributedHelpers.apply_write(partition_idx, partition, [{:put, records}])

      assert result == :ok
      assert Storage.get(:batch_a, partition) == [{:batch_a, 1}]
      assert Storage.get(:batch_b, partition) == [{:batch_b, 2}]
    end
  end

  # ── has_partition?/1 ─────────────────────────────────────────────────────────

  describe "has_partition?/1" do
    test "returns true for local partitions in local mode" do
      # In local mode, the local node is the primary for all partitions
      for idx <- 0..3 do
        assert DistributedHelpers.has_partition?(idx) == true
      end
    end

    test "returns true for partition 0" do
      assert DistributedHelpers.has_partition?(0) == true
    end

    test "returns true for all configured partitions" do
      num = Config.get_config(:num_partition, 4)

      for idx <- 0..(num - 1) do
        assert DistributedHelpers.has_partition?(idx) == true
      end
    end
  end

  # ── route_write/4 ───────────────────────────────────────────────────────────

  describe "route_write/4" do
    test "routes write to local node when it is the primary" do
      # In local mode, the local node is always the primary
      partition_idx = 0

      result =
        DistributedHelpers.route_write(
          TestWriter,
          :write,
          [:dh_route_key, "routed_value"],
          partition_idx
        )

      assert result == {:ok, {:dh_route_key, "routed_value"}}
    end

    test "routed write applies data correctly" do
      partition_idx = 0

      DistributedHelpers.route_write(
        TestWriter,
        :write_with_apply,
        [:dh_apply_key, "applied_value", partition_idx],
        partition_idx
      )

      partition = Partition.get_partition_by_idx(partition_idx)

      assert Storage.get({:routed, :dh_apply_key}, partition) == [
               {{:routed, :dh_apply_key}, "applied_value"}
             ]
    end

    test "route_write works for all partitions" do
      num = Config.get_config(:num_partition, 4)

      for idx <- 0..(num - 1) do
        key = :"dh_part_#{idx}"

        DistributedHelpers.route_write(
          TestWriter,
          :write_with_apply,
          [key, idx, idx],
          idx
        )
      end

      for idx <- 0..(num - 1) do
        partition = Partition.get_partition_by_idx(idx)
        key = :"dh_part_#{idx}"
        assert Storage.get({:routed, key}, partition) != []
      end
    end
  end

  # ── route_read/5 ────────────────────────────────────────────────────────────

  describe "route_read/5" do
    setup do
      # Pre-populate some data for read tests
      # Use Partition.get_partition(key) so data lands in the partition
      # where the key actually hashes to — matching how TestReader.read/1 looks it up.
      for i <- 0..3 do
        key = {:dh_read, i}
        partition = Partition.get_partition(key)
        Storage.put({key, "value_#{i}"}, partition)
      end

      :ok
    end

    test "local read mode reads from local node" do
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read,
          [key],
          partition_idx,
          read_mode: :local
        )

      assert result != []
    end

    test "primary read mode reads from primary node" do
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read,
          [key],
          partition_idx,
          read_mode: :primary
        )

      assert result != []
    end

    test "quorum read mode returns majority result" do
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read,
          [key],
          partition_idx,
          read_mode: :quorum
        )

      # In local mode with single node, quorum should still work
      assert result != []
    end

    test "default read mode is :local" do
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read,
          [key],
          partition_idx,
          []
        )

      assert result != []
    end

    test "local mode escalates to primary when node lacks partition" do
      # In local mode, the node always has the partition, so this tests
      # the escalation path indirectly. We verify the result is correct.
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read_value,
          [key],
          partition_idx,
          read_mode: :local
        )

      assert result == "value_0"
    end

    test "all read modes return consistent results" do
      key = {:dh_read, 0}
      partition_idx = Partition.get_partition_order(key)

      local_result =
        DistributedHelpers.route_read(TestReader, :read, [key], partition_idx, read_mode: :local)

      primary_result =
        DistributedHelpers.route_read(TestReader, :read, [key], partition_idx,
          read_mode: :primary
        )

      quorum_result =
        DistributedHelpers.route_read(TestReader, :read, [key], partition_idx, read_mode: :quorum)

      # All modes should return the same data in local mode
      assert local_result == primary_result
      assert local_result == quorum_result
    end
  end

  # ── read_primary/4 ──────────────────────────────────────────────────────────

  describe "read_primary/4" do
    setup do
      partition = Partition.get_partition(:dh_primary_key)
      Storage.put({:dh_primary_key, "primary_value"}, partition)
      :ok
    end

    test "reads from local node when it is the primary" do
      partition_idx = Partition.get_partition_order(:dh_primary_key)

      result =
        DistributedHelpers.read_primary(
          TestReader,
          :read,
          [:dh_primary_key],
          partition_idx
        )

      assert result != []
    end

    test "returns the correct value" do
      partition_idx = Partition.get_partition_order(:dh_primary_key)

      result =
        DistributedHelpers.read_primary(
          TestReader,
          :read_value,
          [:dh_primary_key],
          partition_idx
        )

      assert result == "primary_value"
    end
  end

  # ── read_quorum/4 ───────────────────────────────────────────────────────────

  describe "read_quorum/4" do
    setup do
      for idx <- 0..3 do
        key = {:dh_quorum, idx}
        partition = Partition.get_partition(key)
        Storage.put({key, "quorum_#{idx}"}, partition)
      end

      :ok
    end

    test "returns result from quorum of replicas" do
      key = {:dh_quorum, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.read_quorum(
          TestReader,
          :read,
          [key],
          partition_idx
        )

      assert result != []
    end

    test "returns the correct value via quorum" do
      key = {:dh_quorum, 0}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.read_quorum(
          TestReader,
          :read_value,
          [key],
          partition_idx
        )

      assert result == "quorum_0"
    end

    test "quorum read works for all partitions" do
      for idx <- 0..3 do
        key = {:dh_quorum, idx}
        partition_idx = Partition.get_partition_order(key)

        result =
          DistributedHelpers.read_quorum(
            TestReader,
            :read_value,
            [key],
            partition_idx
          )

        assert result == "quorum_#{idx}"
      end
    end
  end

  # ── Concurrent access ───────────────────────────────────────────────────────

  describe "concurrent operations" do
    test "concurrent apply_write calls" do
      num_writers = 50

      tasks =
        for i <- 1..num_writers do
          Task.async(fn ->
            key = {:dh_concurrent, i}
            partition_idx = Partition.get_partition_order(key)
            partition = Partition.get_partition(key)

            DistributedHelpers.apply_write(partition_idx, partition, [
              {:put, {key, i}}
            ])
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # All writes should succeed
      assert Enum.all?(results, &(&1 == :ok))

      # Verify data was written
      for i <- 1..num_writers do
        key = {:dh_concurrent, i}
        partition = Partition.get_partition(key)
        assert Storage.get(key, partition) != []
      end
    end

    test "concurrent reads and writes" do
      # Pre-populate
      for i <- 1..50 do
        key = {:dh_rw, i}
        partition = Partition.get_partition(key)
        Storage.put({key, i}, partition)
      end

      write_tasks =
        for i <- 51..100 do
          Task.async(fn ->
            key = {:dh_rw, i}
            partition_idx = Partition.get_partition_order(key)
            partition = Partition.get_partition(key)

            DistributedHelpers.apply_write(partition_idx, partition, [
              {:put, {key, i}}
            ])
          end)
        end

      read_tasks =
        for i <- 1..50 do
          Task.async(fn ->
            key = {:dh_rw, i}
            partition_idx = Partition.get_partition_order(key)

            DistributedHelpers.route_read(
              TestReader,
              :read,
              [key],
              partition_idx,
              read_mode: :local
            )
          end)
        end

      write_results = Task.await_many(write_tasks, 10_000)
      read_results = Task.await_many(read_tasks, 10_000)

      assert Enum.all?(write_results, &(&1 == :ok))
      assert Enum.all?(read_results, &is_list/1)
    end
  end

  # ── Edge cases ──────────────────────────────────────────────────────────────

  describe "edge cases" do
    test "apply_write with nil value" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [{:put, {:dh_nil, nil}}])

      assert result == :ok
      assert Storage.get(:dh_nil, partition) == [{:dh_nil, nil}]
    end

    test "apply_write with complex nested data" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      complex_data = %{
        nested: %{deep: [1, 2, 3]},
        tuple: {:a, :b, :c}
      }

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [
          {:put, {{:kv, :complex, :key}, complex_data}}
        ])

      assert result == :ok

      assert Storage.get({:kv, :complex, :key}, partition) == [
               {{:kv, :complex, :key}, complex_data}
             ]
    end

    test "delete non-existent key is a no-op" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [
          {:delete, :nonexistent_key}
        ])

      assert result == :ok
    end

    test "delete_match with no matching records is a no-op" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      result =
        DistributedHelpers.apply_write(partition_idx, partition, [
          {:delete_match, {:no_match, :_}}
        ])

      assert result == :ok
    end

    test "route_read with missing data returns empty list" do
      key = {:missing_key, 999}
      partition_idx = Partition.get_partition_order(key)

      result =
        DistributedHelpers.route_read(
          TestReader,
          :read,
          [key],
          partition_idx,
          read_mode: :local
        )

      assert result == []
    end

    test "has_partition? with high partition index" do
      # In local mode, all partitions are owned by the local node
      # But an out-of-range index may return unexpected results
      num = Config.get_config(:num_partition, 4)

      # Within range should be true
      assert DistributedHelpers.has_partition?(num - 1) == true

      # Out of range — Manager.get_replicas returns {node(), []} as default
      # so this should still return true
      assert DistributedHelpers.has_partition?(num + 100) == true
    end
  end

  # ── Integration with real modules ───────────────────────────────────────────

  describe "integration with KeyValue" do
    setup do
      alias SuperCache.KeyValue, as: KV
      KV.add("dh_integration", :key1, "value1")
      KV.add("dh_integration", :key2, "value2")
      :ok
    end

    test "KeyValue data is accessible via route_read" do
      partition_idx = Partition.get_partition_order("dh_integration")

      result =
        DistributedHelpers.route_read(
          SuperCache.KeyValue,
          :local_get,
          ["dh_integration", :key1, nil],
          partition_idx,
          read_mode: :local
        )

      assert result == "value1"
    end

    test "KeyValue data is modifiable via route_write" do
      partition_idx = Partition.get_partition_order("dh_integration")

      DistributedHelpers.route_write(
        SuperCache.KeyValue,
        :local_put,
        ["dh_integration", :key3, "value3"],
        partition_idx
      )

      alias SuperCache.KeyValue, as: KV
      assert KV.get("dh_integration", :key3) == "value3"
    end
  end
end
