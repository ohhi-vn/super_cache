defmodule SuperCache.PartitionTest do
  use ExUnit.Case

  alias SuperCache.{Partition, Config}

  @data {:a, "hello"}

  setup_all do
    unless SuperCache.started?() do
      SuperCache.start!()
    end
    :ok
  end

  test "get partition" do
    order = Partition.get_pattition_order(@data)
    prefix = Config.get_config(:table_prefix)
    partition = String.to_atom("#{prefix}_#{order}")
    assert  partition == Partition.get_partition(@data)
  end

  test "get all partitions" do
    partitions = Partition.get_all_partition()
    assert length(partitions) == Partition.get_schedulers()
  end
end
