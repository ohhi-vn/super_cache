defmodule SuperCache.StorageTest do
  use ExUnit.Case

  alias SuperCache.Storage

  @data  {:a, 1, "hello"}
  @partition :"SuperCache.Storage.Ets_0"

  setup_all do
    unless SuperCache.started?() do
      SuperCache.start!()
    end
    :ok
  end

  setup do
    Storage.delete_all(@partition)
    :ok
  end

  test "store data" do
    Storage.put(@data, @partition)
    assert [@data] == Storage.get(:a, @partition)
  end

  test "get data by match" do
    Storage.put(@data, @partition)
    assert [1] == List.flatten(Storage.get_by_match({:a, :"$1", :_}, @partition))
  end

  test "get data by match_object" do
    Storage.put(@data, @partition)
    assert [@data] == Storage.get_by_match_object({:a, :_, :_}, @partition)
  end

  test "delete data" do
    Storage.put(@data, @partition)
    Storage.delete(:a, @partition)
    [] = Storage.get({}, @partition)
  end

  test "delete all data" do
    Storage.put(@data, @partition)
    Storage.delete_all(@partition)
    assert ([] == Storage.get({}, @partition))
  end

  test "scan data" do
    Storage.put(@data, @partition)
   assert 1 == Storage.scan(fn {_, n, _}, acc ->
      acc + n
    end, 0, @partition)
  end
end
