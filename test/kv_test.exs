defmodule SuperCache.KeyValueTest do
  use ExUnit.Case
  doctest SuperCache

  alias SuperCache.KeyValue, as: KV

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

  test "add & get data" do
    data = "Hello"
    KV.add("my_kv", :key, data)
    result = KV.get("my_kv", :key)
    assert data == result
  end

  test "add overwrite old data" do
    data = "Hello"
    KV.add("my_kv", :key, data)
    data = "World"
    KV.add("my_kv", :key, data)

    result = KV.get("my_kv", :key)
    assert data == result
  end

  test "remove data" do
    data = "Hello"
    KV.add("my_kv", :key, data)
    KV.remove("my_kv", :key)
    result = KV.get("my_kv", :key)
    assert data != result
  end

  test "remove all data" do
    data = "Hello"
    KV.add("my_kv", :key, data)
    KV.add("my_kv", :key2, data)
    KV.remove_all("my_kv")
    result = KV.count("my_kv")
    assert data != result
  end

  test "get with default data" do
    data = "Hello"
    result = KV.get("my_kv", :key, data)
    assert data == result
  end

  test "count item" do
    Enum.each(1..10, fn i -> KV.add("my_kv", i, i) end)
    result = KV.count("my_kv")
    assert 10 == result
  end

  test "to_list" do
    Enum.each(1..10, fn i -> KV.add("my_kv", i, i) end)
    result = KV.to_list("my_kv")
    assert Enum.count(result) == 10
  end
end
