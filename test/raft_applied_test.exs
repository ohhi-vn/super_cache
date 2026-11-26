defmodule SuperCache.Cluster.Raft.AppliedTest do
  use ExUnit.Case, async: false

  alias SuperCache.Cluster.Raft.Applied

  setup do
    for partition <- [101, 102], do: Applied.reset(partition)
    :ok
  end

  test "get defaults to zero for an unknown partition" do
    assert Applied.get(101) == 0
  end

  test "claim accepts newer indexes and rejects replays" do
    assert Applied.claim(101, 1)
    assert Applied.get(101) == 1
    refute Applied.claim(101, 1)
    refute Applied.claim(101, 0)
    assert Applied.claim(101, 2)
    assert Applied.get(101) == 2
  end

  test "reset_all clears watermarks when storage tables are recreated" do
    Applied.record(101, 5)
    Applied.record(102, 9)

    assert :ok = Applied.reset_all()

    assert Applied.get(101) == 0
    assert Applied.get(102) == 0
  end

  test "replaying a lower or equal index does not re-apply an increment" do
    counter = :ets.new(:raft_apply_counter, [:set, :private])
    :ets.insert(counter, {:increments, 0})

    apply_index = fn index ->
      if Applied.claim(101, index) do
        :ets.update_counter(counter, :increments, 1)
      else
        :skip
      end
    end

    apply_index.(1)
    apply_index.(1)
    assert :skip = apply_index.(1)
    assert :skip = apply_index.(0)
    apply_index.(2)

    assert :ets.lookup(counter, :increments) == [{:increments, 2}]
  end
end
