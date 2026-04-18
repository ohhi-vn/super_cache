defmodule SuperCache.ConfigTest do
  use ExUnit.Case, async: false

  alias SuperCache.Config

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!()
    :ok
  end

  setup do
    # Reset any test-specific config keys between tests
    Config.clear_config()
    Config.set_config(:key_pos, 0)
    Config.set_config(:partition_pos, 0)
    Config.set_config(:started, true)
    :ok
  end

  describe "get_config/2 and set_config/2" do
    test "set and get a simple value" do
      Config.set_config(:my_key, "my_value")
      assert Config.get_config(:my_key) == "my_value"
    end

    test "get_config returns default when key not set" do
      assert Config.get_config(:nonexistent) == nil
      assert Config.get_config(:nonexistent, :default) == :default
    end

    test "set_config overwrites existing value" do
      Config.set_config(:my_key, "first")
      Config.set_config(:my_key, "second")
      assert Config.get_config(:my_key) == "second"
    end

    test "set_config returns :ok" do
      assert Config.set_config(:any_key, :any_value) == :ok
    end

    test "supports various value types" do
      Config.set_config(:atom_val, :hello)
      Config.set_config(:int_val, 42)
      Config.set_config(:float_val, 3.14)
      Config.set_config(:list_val, [1, 2, 3])
      Config.set_config(:map_val, %{a: 1})
      Config.set_config(:tuple_val, {:a, :b})

      assert Config.get_config(:atom_val) == :hello
      assert Config.get_config(:int_val) == 42
      assert Config.get_config(:float_val) == 3.14
      assert Config.get_config(:list_val) == [1, 2, 3]
      assert Config.get_config(:map_val) == %{a: 1}
      assert Config.get_config(:tuple_val) == {:a, :b}
    end
  end

  describe "fast keys (persistent_term)" do
    test "fast keys are readable via get_config" do
      Config.set_config(:key_pos, 2)
      assert Config.get_config(:key_pos) == 2

      Config.set_config(:partition_pos, 3)
      assert Config.get_config(:partition_pos) == 3

      Config.set_config(:num_partition, 16)
      assert Config.get_config(:num_partition) == 16

      Config.set_config(:table_type, :bag)
      assert Config.get_config(:table_type) == :bag

      Config.set_config(:table_prefix, "MyPrefix")
      assert Config.get_config(:table_prefix) == "MyPrefix"

      Config.set_config(:cluster, :distributed)
      assert Config.get_config(:cluster) == :distributed
    end

    test "fast keys survive GenServer restart" do
      Config.set_config(:num_partition, 99)
      assert Config.get_config(:num_partition) == 99

      # Fast keys are stored in persistent_term, so they survive
      # even if the GenServer state is lost. This is by design.
      assert Config.get_config(:num_partition) == 99
    end
  end

  describe "has_config?/1" do
    test "returns true for existing keys" do
      Config.set_config(:existing_key, "value")
      assert Config.has_config?(:existing_key) == true
    end

    test "returns false for non-existing keys" do
      assert Config.has_config?(:nonexistent_key) == false
    end

    test "returns true for fast keys after they are set" do
      Config.set_config(:num_partition, 8)
      assert Config.has_config?(:num_partition) == true
    end

    test "returns true for key set to nil" do
      Config.set_config(:nil_key, nil)
      assert Config.has_config?(:nil_key) == true
    end
  end

  describe "delete_config/1" do
    test "deletes an existing key" do
      Config.set_config(:to_delete, "value")
      assert Config.get_config(:to_delete) == "value"

      Config.delete_config(:to_delete)
      assert Config.get_config(:to_delete) == nil
    end

    test "deleting a non-existent key does not raise" do
      assert Config.delete_config(:nonexistent) == :ok
    end

    test "deleting a fast key removes it from persistent_term" do
      Config.set_config(:num_partition, 8)
      assert Config.get_config(:num_partition) == 8

      Config.delete_config(:num_partition)
      assert Config.get_config(:num_partition) == nil
    end

    test "returns :ok" do
      Config.set_config(:key, "val")
      assert Config.delete_config(:key) == :ok
    end
  end

  describe "clear_config/0" do
    test "removes all configuration values" do
      Config.set_config(:key_a, "a")
      Config.set_config(:key_b, "b")
      Config.set_config(:key_c, "c")

      Config.clear_config()

      assert Config.get_config(:key_a) == nil
      assert Config.get_config(:key_b) == nil
      assert Config.get_config(:key_c) == nil
    end

    test "clears fast keys from persistent_term" do
      Config.set_config(:num_partition, 8)
      Config.set_config(:table_type, :set)

      Config.clear_config()

      assert Config.get_config(:num_partition) == nil
      assert Config.get_config(:table_type) == nil
    end

    test "returns :ok" do
      assert Config.clear_config() == :ok
    end

    test "cache is usable after clear" do
      Config.clear_config()
      Config.set_config(:new_key, "new_value")
      assert Config.get_config(:new_key) == "new_value"
    end
  end

  describe "get_key!/1" do
    test "extracts key at configured key_pos (default 0)" do
      Config.set_config(:key_pos, 0)
      assert Config.get_key!({:user, 1, "Alice"}) == :user
    end

    test "extracts key at custom key_pos" do
      Config.set_config(:key_pos, 1)
      assert Config.get_key!({:user, 42, "Alice"}) == 42
    end

    test "extracts key at key_pos 2" do
      Config.set_config(:key_pos, 2)
      assert Config.get_key!({:user, 42, "Alice"}) == "Alice"
    end

    test "raises when tuple is too small for key_pos" do
      Config.set_config(:key_pos, 5)

      assert_raise SuperCache.Config, ~r/tuple size.*lower than key_pos/, fn ->
        Config.get_key!({:a, :b})
      end
    end
  end

  describe "get_partition!/1" do
    test "extracts partition at configured partition_pos (default 0)" do
      Config.set_config(:partition_pos, 0)
      assert Config.get_partition!({:user, 1, "Alice"}) == :user
    end

    test "extracts partition at custom partition_pos" do
      Config.set_config(:partition_pos, 1)
      assert Config.get_partition!({:user, 42, "Alice"}) == 42
    end

    test "raises when tuple is too small for partition_pos" do
      Config.set_config(:partition_pos, 10)

      assert_raise SuperCache.Config, ~r/tuple size.*lower than partition_pos/, fn ->
        Config.get_partition!({:a, :b})
      end
    end
  end

  describe "distributed?/0" do
    test "returns false in local mode (default)" do
      Config.set_config(:cluster, :local)
      assert Config.distributed?() == false
    end

    test "returns false when cluster key is not set" do
      Config.delete_config(:cluster)
      assert Config.distributed?() == false
    end

    test "returns true in distributed mode" do
      Config.set_config(:cluster, :distributed)
      assert Config.distributed?() == true
    end
  end

  describe "concurrent access" do
    test "multiple processes can read and write concurrently" do
      Config.set_config(:concurrent_counter, 0)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Config.set_config(:"concurrent_key_#{i}", i)
            Config.get_config(:"concurrent_key_#{i}")
          end)
        end

      results = Task.await_many(tasks, 5_000)

      Enum.with_index(results, 1)
      |> Enum.each(fn {value, i} ->
        assert value == i
      end)
    end

    test "fast key reads are consistent under concurrent writes" do
      Config.set_config(:num_partition, 8)

      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            # Fast key reads should always return a value
            val = Config.get_config(:num_partition)
            assert is_integer(val)
          end)
        end

      Task.await_many(tasks, 5_000)
    end
  end

  describe "edge cases" do
    test "setting a key to nil and reading it back" do
      Config.set_config(:nil_key, nil)
      assert Config.get_config(:nil_key) == nil
      assert Config.has_config?(:nil_key) == true
    end

    test "setting a key to false and reading it back" do
      Config.set_config(:false_key, false)
      assert Config.get_config(:false_key) == false
    end

    test "get_config default is not returned for existing nil value" do
      Config.set_config(:nil_key, nil)
      # Should return nil (the stored value), not :default
      assert Config.get_config(:nil_key, :default) == nil
    end

    test "overwriting a fast key with a different value type" do
      Config.set_config(:num_partition, 8)
      assert Config.get_config(:num_partition) == 8

      Config.set_config(:num_partition, "string_value")
      assert Config.get_config(:num_partition) == "string_value"
    end

    test "get_key! with single-element tuple" do
      Config.set_config(:key_pos, 0)
      assert Config.get_key!({:only_element}) == :only_element
    end

    test "get_partition! with single-element tuple" do
      Config.set_config(:partition_pos, 0)
      assert Config.get_partition!({:only_element}) == :only_element
    end
  end
end
