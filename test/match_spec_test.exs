defmodule SuperCache.Storage.MatchSpecTest do
  use ExUnit.Case, async: false

  alias SuperCache.Storage
  alias SuperCache.Storage.MatchSpec

  @partition :"SuperCache.Storage.Ets_0"

  setup_all do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

    :ok
  end

  setup do
    Storage.delete_all(@partition)
    :ok
  end

  test "match/2 returns binding lists via select" do
    Storage.put({:a, 1, "hello"}, @partition)
    spec = MatchSpec.match({:a, :"$1", :_}, [:"$1"])

    assert [[1]] == MatchSpec.select(@partition, spec)
  end

  test "match_object/1 returns full records" do
    Storage.put({:a, 1, "hello"}, @partition)
    spec = MatchSpec.match_object({:a, :_, :_})

    assert [{:a, 1, "hello"}] == MatchSpec.select(@partition, spec)
  end

  test "delete_match/1 removes only matching records" do
    Storage.put({:a, 1, "hello"}, @partition)
    Storage.put({:b, 2, "world"}, @partition)

    spec = MatchSpec.delete_match({:a, :_, "hello"})
    assert 1 == MatchSpec.select_delete(@partition, spec)

    remaining = Storage.get_by_match_object({:a, :_, :_}, @partition)
    assert [] == remaining

    assert [{:b, 2, "world"}] == Storage.get_by_match_object({:b, :_, :_}, @partition)
  end

  test "compile/1 returns a term (deprecated — unusable on OTP 28+)" do
    # apply/3 avoids triggering the deprecation warning inside our own suite.
    compiled = apply(MatchSpec, :compile, [MatchSpec.match_object({:a, :_, :_})])
    assert compiled
  end

  test "all_records/0 selects whole records bound to $1" do
    Storage.put({:r, 9}, @partition)

    assert [{:r, 9}] == MatchSpec.select(@partition, MatchSpec.all_records())
  end

  test "with_guard/3 filters by guard condition" do
    Storage.put({:g, 5, "low"}, @partition)
    Storage.put({:g, 150, "high"}, @partition)

    spec = MatchSpec.with_guard({:"$1", :"$2", :"$3"}, [{:>, :"$2", 100}], [:"$_"])

    assert [{:g, 150, "high"}] == MatchSpec.select(@partition, spec)
  end
end
