defmodule SuperCache.Partition do
  require Logger

  alias SuperCache.Partition.{Common, Holder}

  def get_partition(data) do
    order = Common.get_pattition_order(data)
    Holder.get(order)
  end

  def start(num_partition) when is_integer(num_partition) do
    Common.set_num_partition(num_partition)
    Enum.each(0..num_partition - 1, fn order ->
      Holder.set(order)
    end)
  end

  def stop() do
    Holder.clean()
  end
end
