defmodule SuperCacheTest do
  use ExUnit.Case
  doctest SuperCache

  @data {:a, :b, :c, 1, "Hello"}

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

  test "put & failed" do
    {:error, _} = SuperCache.put({})
    true
  end

  test "put & get data" do
    SuperCache.put!(@data)
    result = SuperCache.get!(@data)
    assert [@data] == result
  end

  test "get data by key & partition" do
    SuperCache.put!(@data)
    result = SuperCache.get_by_key_partition!(:a, :a)
    assert [@data] == result
  end

  test "get data by same key & partition" do
    SuperCache.put!(@data)
    result = SuperCache.get_same_key_partition!(:a)
    assert [@data] == result
  end

  test "get data by match a pattern" do
    SuperCache.put!(@data)
    result = SuperCache.get_by_match!({:_, :_, :c, :_, :"$1"})
    assert ["Hello"] == List.flatten(result)
  end

  test "delete data by tuple data" do
    SuperCache.put!(@data)
    result = SuperCache.get!(@data)
    assert [@data] == result

    SuperCache.delete!(@data)
    result = SuperCache.get!(@data)
    assert [] == result
  end

  test "delete data by key & pattern" do
    SuperCache.put!(@data)
    result = SuperCache.get!(@data)
    assert [@data] == result

    SuperCache.delete_by_key_partition!(:a, :a)
    result = SuperCache.get!(@data)
    assert [] == result
  end

  test "delete data by same key & pattern" do
    SuperCache.put!(@data)
    result = SuperCache.get!(@data)
    assert [@data] == result

    SuperCache.delete_same_key_partition!(:a)
    result = SuperCache.get!(@data)
    assert [] == result
  end

  test "scan data with a fun" do
    fun = fn ({_, _, _, n, _}, acc) ->
      n + acc
    end
    SuperCache.put!(@data)
    result = SuperCache.scan!(fun, 0)
    assert 1 == result
  end

  test "scan data in a partition with a fun" do
    fun = fn ({_, _, _, n, _}, acc) ->
      n + acc
    end
    SuperCache.put!(@data)
    result = SuperCache.scan!(:a, fun, 0)
    assert 1 == result
  end
end
