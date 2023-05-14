defmodule SuperCache.PartitionTest do
  use ExUnit.Case

  alias SuperCache.Partition

  @data {:a, "hello"}

  setup_all do
    unless SuperCache.started?() do
      SuperCache.start!()
    end
    :ok
  end

  test "get partition" do
    order = Partition.Common.get_pattition_order(@data)
    assert  String.to_atom("supercache_partition_#{order}") == Partition.get_partition(@data)
  end

  test "get all partitions" do
    partitions = Partition.get_all_partition()
    assert length(partitions) == Partition.Common.get_schedulers()
  end
end
