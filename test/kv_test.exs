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

  test "get with default data" do
    data = "Hello"
    result = KV.get("my_kv", :key, data)
    assert data == result
  end
end
