defmodule SuperCache.BootstrapTest do
  @moduledoc """
  Tests for SuperCache.Bootstrap — the unified startup/shutdown module.

  ## Test categories

  - **Lifecycle**: start!/stop, multiple cycles, started?/0
  - **Default options**: start!() with no arguments
  - **Custom options**: key_pos, partition_pos, num_partition, table_type
  - **Validation**: invalid options raise ArgumentError
  - **Idempotency**: calling stop/0 when not started
  - **Edge cases**: empty tuples, boundary values
  """

  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Config, Partition}

  # Top-level struct to avoid nested module issues inside test blocks.
  defmodule BootstrapTestUser do
    @moduledoc false
    defstruct [:id, :name]
  end

  # Ensure clean state before each test
  setup do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    on_exit(fn ->
      if SuperCache.started?() do
        SuperCache.stop()
        Process.sleep(100)
      end
    end)

    :ok
  end

  # ── Lifecycle ────────────────────────────────────────────────────────────────

  describe "start!/0 and stop/0 lifecycle" do
    test "start!/0 starts with default options" do
      assert Bootstrap.start!() == :ok
      assert SuperCache.started?() == true
    end

    test "stop/0 stops the cache" do
      Bootstrap.start!()
      assert SuperCache.started?() == true

      assert Bootstrap.stop() == :ok
      assert SuperCache.started?() == false
    end

    test "started?/0 returns false before start" do
      assert SuperCache.started?() == false
    end

    test "started?/0 returns true after start" do
      Bootstrap.start!()
      assert SuperCache.started?() == true
    end

    test "started?/0 returns false after stop" do
      Bootstrap.start!()
      Bootstrap.stop()
      assert SuperCache.started?() == false
    end

    test "multiple start/stop cycles work" do
      for _ <- 1..3 do
        Bootstrap.start!()
        assert SuperCache.started?() == true

        Bootstrap.stop()
        assert SuperCache.started?() == false

        Process.sleep(50)
      end
    end

    test "cache is usable after restart" do
      Bootstrap.start!()
      SuperCache.put!({:restart_test, "value1"})
      assert SuperCache.get!({:restart_test, nil}) != []

      Bootstrap.stop()
      Process.sleep(50)

      Bootstrap.start!()
      # After restart, data is cleared (new ETS tables)
      assert SuperCache.get!({:restart_test, nil}) == []

      # But we can write new data
      SuperCache.put!({:restart_test, "value2"})
      assert SuperCache.get!({:restart_test, nil}) != []
    end
  end

  # ── Default options ──────────────────────────────────────────────────────────

  describe "default options" do
    test "key_pos defaults to 0" do
      Bootstrap.start!()
      assert Config.get_config(:key_pos) == 0
    end

    test "partition_pos defaults to 0" do
      Bootstrap.start!()
      assert Config.get_config(:partition_pos) == 0
    end

    test "cluster defaults to :local" do
      Bootstrap.start!()
      assert Config.get_config(:cluster) == :local
    end

    test "table_type defaults to :set" do
      Bootstrap.start!()
      assert Config.get_config(:table_type) == :set
    end

    test "num_partition defaults to scheduler count" do
      Bootstrap.start!()
      assert Config.get_config(:num_partition) == System.schedulers_online()
    end

    test "table_prefix defaults to SuperCache.Storage.Ets" do
      Bootstrap.start!()
      assert Config.get_config(:table_prefix) == "SuperCache.Storage.Ets"
    end
  end

  # ── Custom options ───────────────────────────────────────────────────────────

  describe "start!/1 with custom options" do
    test "custom key_pos" do
      Bootstrap.start!(key_pos: 1, partition_pos: 1)
      assert Config.get_config(:key_pos) == 1
    end

    test "custom partition_pos" do
      Bootstrap.start!(key_pos: 0, partition_pos: 2)
      assert Config.get_config(:partition_pos) == 2
    end

    test "custom num_partition" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      assert Config.get_config(:num_partition) == 4
    end

    test "custom table_type :bag" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :bag)
      assert Config.get_config(:table_type) == :bag
    end

    test "custom table_type :ordered_set" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :ordered_set)
      assert Config.get_config(:table_type) == :ordered_set
    end

    test "custom table_type :duplicate_bag" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :duplicate_bag)
      assert Config.get_config(:table_type) == :duplicate_bag
    end

    test "custom table_prefix" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_prefix: "MyCache")
      assert Config.get_config(:table_prefix) == "MyCache"
    end

    test "cluster :local is explicit" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, cluster: :local)
      assert Config.get_config(:cluster) == :local
    end

    test "all options together" do
      Bootstrap.start!(
        key_pos: 1,
        partition_pos: 2,
        num_partition: 8,
        table_type: :bag,
        table_prefix: "TestCache",
        cluster: :local
      )

      assert Config.get_config(:key_pos) == 1
      assert Config.get_config(:partition_pos) == 2
      assert Config.get_config(:num_partition) == 8
      assert Config.get_config(:table_type) == :bag
      assert Config.get_config(:table_prefix) == "TestCache"
      assert Config.get_config(:cluster) == :local
    end
  end

  # ── Validation ───────────────────────────────────────────────────────────────

  describe "validation errors" do
    test "raises on non-keyword options" do
      assert_raise ArgumentError, ~r/options must be a keyword list/, fn ->
        Bootstrap.start!("not a keyword list")
      end
    end

    test "raises on missing key_pos" do
      assert_raise ArgumentError, ~r/missing required option: :key_pos/, fn ->
        Bootstrap.start!(partition_pos: 0)
      end
    end

    test "raises on missing partition_pos" do
      assert_raise ArgumentError, ~r/missing required option: :partition_pos/, fn ->
        Bootstrap.start!(key_pos: 0)
      end
    end

    test "raises on negative key_pos" do
      assert_raise ArgumentError, ~r/key_pos must be a non-negative integer/, fn ->
        Bootstrap.start!(key_pos: -1, partition_pos: 0)
      end
    end

    test "raises on non-integer key_pos" do
      assert_raise ArgumentError, ~r/key_pos must be a non-negative integer/, fn ->
        Bootstrap.start!(key_pos: "zero", partition_pos: 0)
      end
    end

    test "raises on negative partition_pos" do
      assert_raise ArgumentError, ~r/partition_pos must be a non-negative integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: -1)
      end
    end

    test "raises on non-integer partition_pos" do
      assert_raise ArgumentError, ~r/partition_pos must be a non-negative integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 1.5)
      end
    end

    test "raises on invalid cluster mode" do
      assert_raise ArgumentError, ~r/unsupported cluster mode/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, cluster: :invalid)
      end
    end

    test "raises on invalid table_type" do
      assert_raise ArgumentError, ~r/unsupported table_type/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :invalid)
      end
    end

    test "raises on zero num_partition" do
      assert_raise ArgumentError, ~r/num_partition must be a positive integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 0)
      end
    end

    test "raises on negative num_partition" do
      assert_raise ArgumentError, ~r/num_partition must be a positive integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: -1)
      end
    end

    test "raises on non-integer num_partition" do
      assert_raise ArgumentError, ~r/num_partition must be a positive integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: "four")
      end
    end

    test "raises on zero replication_factor" do
      assert_raise ArgumentError, ~r/replication_factor must be a positive integer/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, replication_factor: 0)
      end
    end

    test "raises on invalid replication_mode" do
      assert_raise ArgumentError, ~r/unsupported replication_mode/, fn ->
        Bootstrap.start!(key_pos: 0, partition_pos: 0, replication_mode: :invalid)
      end
    end

    test "valid key_pos of 0" do
      assert Bootstrap.start!(key_pos: 0, partition_pos: 0) == :ok
    end

    test "valid large key_pos" do
      # key_pos just needs to be non-negative; the actual tuple size check
      # happens at runtime in Config.get_key!/1
      assert Bootstrap.start!(key_pos: 100, partition_pos: 0) == :ok
    end
  end

  # ── Partition creation ───────────────────────────────────────────────────────

  describe "partition creation" do
    test "creates the specified number of partitions" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

      partitions = Partition.get_all_partition()
      assert length(partitions) == 4
    end

    test "partition names follow the configured prefix" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 2, table_prefix: "Test")

      partitions = Partition.get_all_partition()
      assert :Test_0 in partitions
      assert :Test_1 in partitions
    end

    test "single partition works" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 1)

      partitions = Partition.get_all_partition()
      assert length(partitions) == 1
    end

    test "many partitions work" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 32)

      partitions = Partition.get_all_partition()
      assert length(partitions) == 32
    end
  end

  # ── Stop behavior ────────────────────────────────────────────────────────────

  describe "stop/0" do
    test "stop when not started returns :ok" do
      # Should not raise even if cache was never started
      assert Bootstrap.stop() == :ok
    end

    test "double stop returns :ok" do
      Bootstrap.start!()
      assert Bootstrap.stop() == :ok
      assert Bootstrap.stop() == :ok
    end

    test "stop clears the started flag" do
      Bootstrap.start!()
      assert Config.get_config(:started) == true

      Bootstrap.stop()
      assert Config.get_config(:started) == false
    end

    test "stop cleans up partitions" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

      # Verify partitions exist
      assert length(Partition.get_all_partition()) == 4

      Bootstrap.stop()

      # After stop, partition registry should be clean
      # (Partition.Holder is cleaned during stop)
      assert Partition.get_all_partition() == []
    end
  end

  # ── Data operations after start ──────────────────────────────────────────────

  describe "data operations after bootstrap" do
    test "can put and get data after start" do
      Bootstrap.start!()

      SuperCache.put!({:test, 1, "hello"})
      result = SuperCache.get!({:test, 1, nil})
      assert result != []
    end

    test "can use KeyValue after start" do
      Bootstrap.start!()

      alias SuperCache.KeyValue
      KeyValue.add("test_ns", :key, "value")
      assert KeyValue.get("test_ns", :key) == "value"
    end

    test "can use Queue after start" do
      Bootstrap.start!()

      alias SuperCache.Queue
      Queue.add("test_q", :item1)
      Queue.add("test_q", :item2)
      assert Queue.out("test_q") == :item1
    end

    test "can use Stack after start" do
      Bootstrap.start!()

      alias SuperCache.Stack
      Stack.push("test_s", :bottom)
      Stack.push("test_s", :top)
      assert Stack.pop("test_s") == :top
    end

    test "can use Struct after start" do
      Bootstrap.start!()

      alias SuperCache.Struct
      Struct.init(%BootstrapTestUser{}, :id)

      user = %BootstrapTestUser{id: 1, name: "Alice"}
      Struct.add(user)
      assert {:ok, ^user} = Struct.get(%BootstrapTestUser{id: 1})
    end
  end

  # ── Table type behavior ──────────────────────────────────────────────────────

  describe "table_type configuration" do
    test ":set table overwrites duplicate keys" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :set)

      SuperCache.put!({:dup, 1})
      SuperCache.put!({:dup, 2})
      result = SuperCache.get!({:dup, nil})
      assert length(result) == 1
      assert result == [{:dup, 2}]
    end

    test ":bag table allows duplicate keys with different values" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :bag)

      SuperCache.put!({:dup, 1})
      SuperCache.put!({:dup, 2})
      result = SuperCache.get!({:dup, nil})
      assert length(result) == 2
    end

    test ":duplicate_bag table allows exact duplicate records" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, table_type: :duplicate_bag)

      SuperCache.put!({:dup, 1})
      SuperCache.put!({:dup, 1})
      result = SuperCache.get!({:dup, nil})
      assert length(result) == 2
    end
  end

  # ── Edge cases ───────────────────────────────────────────────────────────────

  describe "edge cases" do
    test "start with key_pos == partition_pos" do
      # This is the default and most common configuration
      assert Bootstrap.start!(key_pos: 0, partition_pos: 0) == :ok

      # Data with the same key goes to the same partition
      SuperCache.put!({:same, "data"})
      assert SuperCache.get!({:same, nil}) != []
    end

    test "start with different key_pos and partition_pos" do
      assert Bootstrap.start!(key_pos: 0, partition_pos: 1) == :ok

      # Partition is determined by element at index 1
      # Key is determined by element at index 0
      SuperCache.put!({:my_key, :my_partition, "data"})
      assert SuperCache.get!({:my_key, :my_partition, nil}) != []
    end

    test "restart with different configuration" do
      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
      assert Config.get_config(:num_partition) == 4

      Bootstrap.stop()
      Process.sleep(50)

      Bootstrap.start!(key_pos: 0, partition_pos: 0, num_partition: 8)
      assert Config.get_config(:num_partition) == 8
    end

    test "lazy_put works after start" do
      Bootstrap.start!()

      assert SuperCache.lazy_put({:lazy, "data"}) == :ok
      # Give buffer time to flush
      Process.sleep(100)
      assert SuperCache.get!({:lazy, nil}) != []
    end

    test "stats works after start" do
      Bootstrap.start!()

      stats = SuperCache.stats()
      assert is_list(stats)
      assert Keyword.has_key?(stats, :total)
    end

    test "cluster_stats works in local mode" do
      Bootstrap.start!()

      stats = SuperCache.cluster_stats()
      assert is_map(stats)
      assert stats.node_count == 1
      assert stats.replication_mode == :none
    end
  end

  # ── Concurrent start/stop ────────────────────────────────────────────────────

  describe "concurrent operations" do
    test "concurrent writes after start" do
      Bootstrap.start!()

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            SuperCache.put!({i, :concurrent, "value_#{i}"})
          end)
        end

      Task.await_many(tasks, 5_000)

      # All writes should have succeeded
      stats = SuperCache.stats()
      assert Keyword.get(stats, :total) == 100
    end

    test "concurrent reads and writes" do
      Bootstrap.start!()

      # Pre-populate some data
      for i <- 1..50 do
        SuperCache.put!({i, :rw, "initial_#{i}"})
      end

      # Concurrent reads and writes
      write_tasks =
        for i <- 51..100 do
          Task.async(fn ->
            SuperCache.put!({i, :rw, "write_#{i}"})
          end)
        end

      read_tasks =
        for i <- 1..50 do
          Task.async(fn ->
            SuperCache.get!({i, :rw, nil})
          end)
        end

      Task.await_many(write_tasks ++ read_tasks, 5_000)

      # All data should be accessible
      stats = SuperCache.stats()
      assert Keyword.get(stats, :total) >= 100
    end
  end
end
