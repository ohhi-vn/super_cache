defmodule SuperCache.Partition do
  require Logger

  alias SuperCache.Partition.{Common, Holder}

  def get_partition(data) do
    order = Common.get_pattition_order(data)
    Holder.get(order)
  end

  def set_partition(data) do
    order = Common.get_pattition_order(data)
    Holder.set(order)
  end
end
