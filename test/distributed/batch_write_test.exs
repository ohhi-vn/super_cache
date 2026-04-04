defmodule SuperCache.Distributed.BatchWriteTest do
  @moduledoc """
  Tests for batch write APIs:
  - SuperCache.put_batch!/1
  - KeyValue.add_batch/2
  - KeyValue.remove_batch/2
  """

  use ExUnit.Case, async: false

  alias SuperCache.{KeyValue, Partition}

  # Use unique namespace per test run to avoid interference from parallel tests
  defp ns(suffix), do: "batch_test_#{suffix}_#{System.unique_integer([:positive])}"

  setup do
    # Always ensure we're running in local mode for these tests
    if SuperCache.started?() do
      SuperCache.stop()
    end
    SuperCache.start!(key_pos: 0, partition_pos: 0)

    on_exit(fn ->
      try do
        SuperCache.stop()
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  describe "SuperCache.put_batch!/1" do
    test "writes multiple tuples and they are retrievable" do
      data = [
        {:batch_user, 1, "Alice"},
        {:batch_user, 2, "Bob"},
        {:batch_user, 3, "Charlie"}
      ]

      assert :ok = SuperCache.put_batch!(data)

      Enum.each(data, fn tuple ->
        key = elem(tuple, 0)
        assert [{^key, _, _}] = SuperCache.get!({key, nil})
      end)
    end

    test "handles empty list" do
      assert :ok = SuperCache.put_batch!([])
    end

    test "overwrites existing records with same key" do
      SuperCache.put!({:batch_user, 1, "Old"})
      assert [{:batch_user, 1, "Old"}] = SuperCache.get!({:batch_user, nil})

      SuperCache.put_batch!([{:batch_user, 1, "New"}])
      assert [{:batch_user, 1, "New"}] = SuperCache.get!({:batch_user, nil})
    end

    test "handles data across multiple partitions" do
      # Use unique keys so each record is individually retrievable
      data = for i <- 1..1000, do: {String.to_atom("batch_multi_#{i}"), i, "value_" <> Integer.to_string(i)}

      assert :ok = SuperCache.put_batch!(data)

      # Verify a sample from different partitions
      Enum.each([1, 500, 1000], fn i ->
        key = String.to_atom("batch_multi_#{i}")
        result = SuperCache.get!({key, nil})
        assert length(result) == 1
        {^key, idx, val} = hd(result)
        assert idx == i
        assert val == "value_" <> Integer.to_string(i)
      end)
    end

    test "returns :ok for successful batch write" do
      assert :ok = SuperCache.put_batch!([{:test, 1}])
    end
  end

  describe "KeyValue.add_batch/2" do
    test "adds multiple key-value pairs" do
      kv_ns = ns("add_batch")
      pairs = [
        {:k1, "v1"},
        {:k2, "v2"},
        {:k3, "v3"}
      ]

      assert :ok = KeyValue.add_batch(kv_ns, pairs)

      Enum.each(pairs, fn {key, value} ->
        assert KeyValue.get(kv_ns, key) == value
      end)
    end

    test "handles empty list" do
      kv_ns = ns("add_batch_empty")
      assert :ok = KeyValue.add_batch(kv_ns, [])
    end

    test "overwrites existing keys" do
      kv_ns = ns("add_batch_overwrite")
      # Ensure clean state for this test
      KeyValue.remove_all(kv_ns)
      KeyValue.add(kv_ns, :key, "old")
      assert KeyValue.get(kv_ns, :key) == "old"

      KeyValue.add_batch(kv_ns, [{:key, "new"}])
      assert KeyValue.get(kv_ns, :key) == "new"
    end
  end

  describe "KeyValue.remove_batch/2" do
    test "removes multiple keys" do
      kv_ns = ns("remove_batch")
      # Ensure clean state for this test
      KeyValue.remove_all(kv_ns)
      KeyValue.add(kv_ns, :a, 1)
      KeyValue.add(kv_ns, :b, 2)
      KeyValue.add(kv_ns, :c, 3)
      KeyValue.add(kv_ns, :d, 4)

      assert :ok = KeyValue.remove_batch(kv_ns, [:a, :c])

      assert KeyValue.get(kv_ns, :a) == nil
      assert KeyValue.get(kv_ns, :c) == nil
      assert KeyValue.get(kv_ns, :b) == 2
      assert KeyValue.get(kv_ns, :d) == 4
    end

    test "handles empty list" do
      kv_ns = ns("remove_batch_empty")
      KeyValue.add(kv_ns, :key, "value")
      assert :ok = KeyValue.remove_batch(kv_ns, [])
      assert KeyValue.get(kv_ns, :key) == "value"
    end

    test "handles non-existent keys gracefully" do
      kv_ns = ns("remove_batch_missing")
      assert :ok = KeyValue.remove_batch(kv_ns, [:no_such_key])
    end
  end
end
