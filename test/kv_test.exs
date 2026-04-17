defmodule SuperCache.KeyValueTest do
  use ExUnit.Case, async: false
  doctest SuperCache

  alias SuperCache.KeyValue, as: KV

  # ── Default (:set table) ────────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!()
    :ok
  end

  setup do
    SuperCache.delete_all()
  end

  describe "basic add & get (:set table)" do
    test "add & get data" do
      data = "Hello"
      KV.add("my_kv", :key, data)
      result = KV.get("my_kv", :key)
      assert data == result
    end

    test "add overwrites old data" do
      KV.add("my_kv", :key, "Hello")
      KV.add("my_kv", :key, "World")
      assert KV.get("my_kv", :key) == "World"
    end

    test "remove data" do
      KV.add("my_kv", :key, "Hello")
      KV.remove("my_kv", :key)
      assert KV.get("my_kv", :key) == nil
    end

    test "remove all data" do
      KV.add("my_kv", :key1, "a")
      KV.add("my_kv", :key2, "b")
      KV.remove_all("my_kv")
      assert KV.count("my_kv") == 0
    end

    test "get with default value" do
      assert KV.get("my_kv", :missing, :default) == :default
    end

    test "count items" do
      Enum.each(1..10, fn i -> KV.add("my_kv", i, i) end)
      assert KV.count("my_kv") == 10
    end

    test "to_list" do
      Enum.each(1..10, fn i -> KV.add("my_kv", i, i) end)
      result = KV.to_list("my_kv")
      assert Enum.count(result) == 10
    end

    test "keys" do
      KV.add("my_kv", :a, 1)
      KV.add("my_kv", :b, 2)
      assert Enum.sort(KV.keys("my_kv")) == [:a, :b]
    end

    test "values" do
      KV.add("my_kv", :a, 1)
      KV.add("my_kv", :b, 2)
      assert Enum.sort(KV.values("my_kv")) == [1, 2]
    end
  end

  describe "update/3 — atomic upsert (:set table)" do
    test "inserts when key does not exist" do
      KV.update("kv", :counter, 0)
      assert KV.get("kv", :counter) == 0
    end

    test "updates when key already exists" do
      KV.add("kv", :counter, 10)
      KV.update("kv", :counter, 20)
      assert KV.get("kv", :counter) == 20
    end

    test "returns :ok" do
      assert KV.update("kv", :key, 1) == :ok
    end

    test "multiple updates are idempotent" do
      KV.update("kv", :k, "v1")
      KV.update("kv", :k, "v2")
      KV.update("kv", :k, "v3")
      assert KV.get("kv", :k) == "v3"
    end
  end

  describe "update/4 — read-modify-write" do
    test "applies function to existing value" do
      KV.add("kv", :score, 10)
      result = KV.update("kv", :score, 0, &(&1 * 2))
      assert result == 20
      assert KV.get("kv", :score) == 20
    end

    test "uses default when key does not exist" do
      result = KV.update("kv", :score, 5, &(&1 + 1))
      assert result == 6
      assert KV.get("kv", :score) == 6
    end

    test "returns the new value" do
      KV.add("kv", :val, "hello")
      result = KV.update("kv", :val, "", &String.upcase/1)
      assert result == "HELLO"
    end

    test "works with complex transformations" do
      KV.add("kv", :cart, %{items: 3, total: 50})

      result =
        KV.update("kv", :cart, %{items: 0, total: 0}, fn cart ->
          %{cart | items: cart.items + 1, total: cart.total + 25}
        end)

      assert result == %{items: 4, total: 75}
    end
  end

  describe "increment/4 — atomic counter" do
    test "starts from default when key does not exist" do
      result = KV.increment("kv", :hits, 0, 1)
      assert result == 1
      assert KV.get("kv", :hits) == 1
    end

    test "increments existing value" do
      KV.add("kv", :hits, 5)
      result = KV.increment("kv", :hits, 0, 1)
      assert result == 6
      assert KV.get("kv", :hits) == 6
    end

    test "increments by custom step" do
      KV.add("kv", :score, 100)
      result = KV.increment("kv", :score, 0, 10)
      assert result == 110
    end

    test "decrements with negative step" do
      KV.add("kv", :lives, 3)
      result = KV.increment("kv", :lives, 0, -1)
      assert result == 2
    end

    test "multiple increments are atomic" do
      KV.add("kv", :counter, 0)

      tasks =
        for _ <- 1..100 do
          Task.async(fn -> KV.increment("kv", :counter, 0, 1) end)
        end

      Task.await_many(tasks, 5_000)
      assert KV.get("kv", :counter) == 100
    end

    test "default value is used as base before first increment" do
      result = KV.increment("kv", :views, 10, 5)
      assert result == 15
    end
  end

  describe "replace/3" do
    test "inserts when key does not exist" do
      KV.replace("kv", :k, "v")
      assert KV.get("kv", :k) == "v"
    end

    test "replaces existing value" do
      KV.add("kv", :k, "old")
      KV.replace("kv", :k, "new")
      assert KV.get("kv", :k) == "new"
    end

    test "returns :ok" do
      assert KV.replace("kv", :k, "v") == :ok
    end
  end

  describe "get_all/3" do
    test "returns empty list when key does not exist" do
      assert KV.get_all("kv", :missing) == []
    end

    test "returns single-element list for :set table" do
      KV.add("kv", :k, "v")
      assert KV.get_all("kv", :k) == ["v"]
    end
  end

  describe "add_batch/2" do
    test "adds multiple key-value pairs" do
      KV.add_batch("kv", a: 1, b: 2, c: 3)
      assert KV.get("kv", :a) == 1
      assert KV.get("kv", :b) == 2
      assert KV.get("kv", :c) == 3
    end

    test "overwrites existing keys on :set table" do
      KV.add("kv", :a, 0)
      KV.add_batch("kv", a: 99)
      assert KV.get("kv", :a) == 99
    end
  end

  describe "remove_batch/2" do
    test "removes multiple keys" do
      KV.add_batch("kv", a: 1, b: 2, c: 3)
      KV.remove_batch("kv", [:a, :c])
      assert KV.get("kv", :a) == nil
      assert KV.get("kv", :b) == 2
      assert KV.get("kv", :c) == nil
    end
  end
end

defmodule SuperCache.KeyValueBagTest do
  use ExUnit.Case, async: false

  alias SuperCache.KeyValue, as: KV

  # ── :bag table type ─────────────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!(key_pos: 0, partition_pos: 0, table_type: :bag)
    :ok
  end

  setup do
    SuperCache.delete_all()
  end

  describe "add & get (:bag table)" do
    test "add inserts duplicate keys" do
      KV.add("kv", :tag, 1)
      KV.add("kv", :tag, 2)
      # get returns the most recently inserted value
      assert KV.get("kv", :tag) == 2
    end

    test "get_all returns all values for a key" do
      KV.add("kv", :tag, 1)
      KV.add("kv", :tag, 2)
      KV.add("kv", :tag, 3)
      assert Enum.sort(KV.get_all("kv", :tag)) == [1, 2, 3]
    end

    test "get returns default when key does not exist" do
      assert KV.get("kv", :missing, :nope) == :nope
    end

    test "get_all returns empty list when key does not exist" do
      assert KV.get_all("kv", :missing) == []
    end

    test "count counts all records including duplicates" do
      KV.add("kv", :a, 1)
      KV.add("kv", :a, 2)
      KV.add("kv", :b, 3)
      assert KV.count("kv") == 3
    end

    test "to_list returns all key-value pairs" do
      KV.add("kv", :a, 1)
      KV.add("kv", :a, 2)
      result = KV.to_list("kv")
      assert Enum.sort(result) == [{:a, 1}, {:a, 2}]
    end

    test "keys returns unique keys" do
      KV.add("kv", :a, 1)
      KV.add("kv", :a, 2)
      KV.add("kv", :b, 3)
      assert Enum.sort(KV.keys("kv")) == [:a, :b]
    end

    test "values returns all values including duplicates" do
      KV.add("kv", :a, 1)
      KV.add("kv", :a, 2)
      assert Enum.sort(KV.values("kv")) == [1, 2]
    end
  end

  describe "update/3 (:bag table)" do
    test "inserts when key does not exist" do
      KV.update("kv", :k, "v")
      assert KV.get_all("kv", :k) == ["v"]
    end

    test "replaces all values for existing key" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 2)
      KV.update("kv", :k, 99)
      assert KV.get_all("kv", :k) == [99]
    end
  end

  describe "replace/3 (:bag table)" do
    test "replaces all values with single value" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 2)
      KV.add("kv", :k, 3)
      KV.replace("kv", :k, 42)
      assert KV.get_all("kv", :k) == [42]
    end

    test "inserts when key does not exist" do
      KV.replace("kv", :new, "val")
      assert KV.get_all("kv", :new) == ["val"]
    end
  end

  describe "update/4 — read-modify-write (:bag table)" do
    test "uses most recent value as current" do
      KV.add("kv", :score, 10)
      KV.add("kv", :score, 20)
      result = KV.update("kv", :score, 0, &(&1 + 5))
      assert result == 25
      # After update, only the new value should remain
      assert KV.get_all("kv", :score) == [25]
    end

    test "uses default when key does not exist" do
      result = KV.update("kv", :val, 100, &(&1 + 1))
      assert result == 101
    end
  end

  describe "increment/4 (:bag table)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, ~r/not supported for :bag/, fn ->
        KV.increment("kv", :counter, 0, 1)
      end
    end
  end

  describe "remove (:bag table)" do
    test "removes all records for a key" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 2)
      KV.remove("kv", :k)
      assert KV.get_all("kv", :k) == []
    end
  end

  describe "remove_all (:bag table)" do
    test "removes all records in namespace" do
      KV.add("kv", :a, 1)
      KV.add("kv", :b, 2)
      KV.add("kv", :a, 3)
      KV.remove_all("kv")
      assert KV.count("kv") == 0
    end
  end

  describe "add_batch/2 (:bag table)" do
    test "adds multiple pairs including duplicates" do
      KV.add_batch("kv", [{:a, 1}, {:a, 2}, {:b, 3}])
      assert Enum.sort(KV.get_all("kv", :a)) == [1, 2]
      assert KV.get_all("kv", :b) == [3]
    end
  end
end

defmodule SuperCache.KeyValueDuplicateBagTest do
  use ExUnit.Case, async: false

  alias SuperCache.KeyValue, as: KV

  # ── :duplicate_bag table type ────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!(key_pos: 0, partition_pos: 0, table_type: :duplicate_bag)
    :ok
  end

  setup do
    SuperCache.delete_all()
  end

  describe "add & get (:duplicate_bag table)" do
    test "allows exact duplicate records" do
      KV.add("kv", :tag, 1)
      KV.add("kv", :tag, 1)
      assert KV.get_all("kv", :tag) == [1, 1]
    end

    test "get returns most recently inserted value" do
      KV.add("kv", :k, "first")
      KV.add("kv", :k, "second")
      assert KV.get("kv", :k) == "second"
    end

    test "get_all returns all values including exact duplicates" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 2)
      assert KV.get_all("kv", :k) == [1, 1, 2]
    end
  end

  describe "replace/3 (:duplicate_bag table)" do
    test "replaces all duplicates with single value" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 1)
      KV.replace("kv", :k, 99)
      assert KV.get_all("kv", :k) == [99]
    end
  end

  describe "increment/4 (:duplicate_bag table)" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, ~r/not supported for :bag\/:duplicate_bag/, fn ->
        KV.increment("kv", :counter, 0, 1)
      end
    end
  end

  describe "remove (:duplicate_bag table)" do
    test "removes all records for a key" do
      KV.add("kv", :k, 1)
      KV.add("kv", :k, 1)
      KV.remove("kv", :k)
      assert KV.get_all("kv", :k) == []
    end
  end
end
