defmodule SuperCache.Partition.HolderTest do
  use ExUnit.Case
\
  alias SuperCache.Partition.Holder

  setup_all do
    unless SuperCache.started?() do
      SuperCache.start!()
    end
    :ok
  end

  setup do
    Holder.clean()
    :ok
  end

  test "set & get partition order" do
    Holder.set(0)
    assert  :"SuperCache.Storage.Ets_0" == Holder.get(0)
  end

  test "get all partitions" do
    Holder.set(0)
    partitions = Holder.get_all()
    assert length(partitions) == 1
  end
end
