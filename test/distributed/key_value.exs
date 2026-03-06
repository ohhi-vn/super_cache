defmodule SuperCache.Distributed.KeyValueTest do
  use ExUnit.Case, async: false

  alias SuperCache.Distributed.KeyValue

  setup_all do
    SuperCache.Cluster.Bootstrap.start!(
      key_pos: 0, partition_pos: 0,
      cluster: :distributed, replication_factor: 2,
      num_partition: 3
    )

    :ok
  end

  setup do
    SuperCache.Distributed.delete_all()
    :ok
  end

  ## add / get ##

  test "add and get a value" do
    KeyValue.add("kv", :name, "Alice")
    assert "Alice" == KeyValue.get("kv", :name)
  end

  test "get returns default when key missing" do
    assert nil == KeyValue.get("kv", :missing)
    assert :default == KeyValue.get("kv", :missing, :default)
  end

  test "add overwrites existing value" do
    KeyValue.add("kv", :counter, 1)
    KeyValue.add("kv", :counter, 2)
    dbg(SuperCache.stats())
    assert 2 == KeyValue.get("kv", :counter)
  end

  test "different namespaces are isolated" do
    KeyValue.add("ns_a", :x, 1)
    KeyValue.add("ns_b", :x, 2)
    assert 1 == KeyValue.get("ns_a", :x)
    assert 2 == KeyValue.get("ns_b", :x)
  end

  test "supports any term as key and value" do
    KeyValue.add("kv", {:compound, 1}, %{data: [1, 2, 3]})
    assert %{data: [1, 2, 3]} == KeyValue.get("kv", {:compound, 1})
  end

  ## remove ##

  test "remove deletes a key" do
    KeyValue.add("kv", :temp, "bye")
    KeyValue.remove("kv", :temp)
    assert nil == KeyValue.get("kv", :temp)
  end

  test "remove on missing key is a no-op" do
    assert :ok == KeyValue.remove("kv", :ghost)
  end

  ## remove_all ##

  test "remove_all clears the namespace" do
    KeyValue.add("kv", :a, 1)
    KeyValue.add("kv", :b, 2)
    KeyValue.remove_all("kv")
    assert nil == KeyValue.get("kv", :a)
    assert nil == KeyValue.get("kv", :b)
  end

  test "remove_all only clears its own namespace" do
    KeyValue.add("kv_keep", :x, 99)
    KeyValue.add("kv_drop", :x, 0)
    KeyValue.remove_all("kv_drop")
    assert 99 == KeyValue.get("kv_keep", :x)
    assert nil == KeyValue.get("kv_drop", :x)
  end

  ## keys / values / count ##

  test "keys returns all keys in namespace" do
    KeyValue.add("meta", :a, 1)
    KeyValue.add("meta", :b, 2)
    assert Enum.sort([:a, :b]) == Enum.sort(KeyValue.keys("meta"))
  end

  test "keys returns empty list for empty namespace" do
    assert [] == KeyValue.keys("empty_ns_kv")
  end

  test "values returns all values in namespace" do
    KeyValue.add("vals", :x, 10)
    KeyValue.add("vals", :y, 20)
    assert Enum.sort([10, 20]) == Enum.sort(KeyValue.values("vals"))
  end

  test "count returns correct number of entries" do
    KeyValue.add("cnt", :a, 1)
    KeyValue.add("cnt", :b, 2)
    KeyValue.add("cnt", :c, 3)
    assert 3 == KeyValue.count("cnt")
  end

  test "count returns 0 for empty namespace" do
    assert 0 == KeyValue.count("empty_cnt_kv")
  end

  test "count decreases after remove" do
    KeyValue.add("cnt2", :a, 1)
    KeyValue.add("cnt2", :b, 2)
    KeyValue.remove("cnt2", :a)
    assert 1 == KeyValue.count("cnt2")
  end

  ## to_list ##

  test "to_list returns key-value pairs" do
    KeyValue.add("list", :p, 7)
    KeyValue.add("list", :q, 8)
    result = KeyValue.to_list("list") |> Enum.sort()
    assert [{:p, 7}, {:q, 8}] == result
  end

  test "to_list returns empty list when namespace is empty" do
    assert [] == KeyValue.to_list("empty_list_kv")
  end

  ## complex ##

  test "complex operations across multiple namespaces" do
    for i <- 1..5 do
      KeyValue.add("complex_a", i, i * 10)
      KeyValue.add("complex_b", i, i * 100)
    end

    assert 5 == KeyValue.count("complex_a")
    assert 5 == KeyValue.count("complex_b")

    KeyValue.remove("complex_a", 3)
    assert 4 == KeyValue.count("complex_a")
    assert 5 == KeyValue.count("complex_b")

    KeyValue.remove_all("complex_a")
    assert 0 == KeyValue.count("complex_a")
    assert 5 == KeyValue.count("complex_b")
  end
end
