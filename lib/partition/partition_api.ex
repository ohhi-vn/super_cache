defmodule SuperCache.Partition do
  @moduledoc false

  require Logger

  alias SuperCache.Partition.{Common, Holder}

  @doc """
  Gets partition from data.
  """
  @spec get_partition(any) :: atom
  def get_partition(data) do
    order = Common.get_pattition_order(data)
    Holder.get(order)
  end

  @doc """
  List all partitions.
  """
  @spec get_all_partition :: [atom]
  def get_all_partition() do
    Holder.get_all()
  end

  @doc """
  Generates all partitions and store it with order is key of partition.
  """
  @spec start(pos_integer) :: :ok
  def start(num_partition) when is_integer(num_partition) do
    Common.set_num_partition(num_partition)
    Enum.each(0..num_partition - 1, fn order ->
      Holder.set(order)
    end)
  end

  @doc """
  Clears all paritions.
  """
  @spec stop :: :ok
  def stop() do
    Holder.clean()
    :ok
  end
end
