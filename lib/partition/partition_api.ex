defmodule SuperCache.Partition do
  @moduledoc false

  require Logger

  alias SuperCache.Partition.Holder

  alias :ets, as: Ets

  @doc """
  Gets partition from data.
  """
  @spec get_partition(any) :: atom
  def get_partition(data) do
    order = get_pattition_order(data)
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
  @spec start(pos_integer) :: any
  def start(num_partition) when is_integer(num_partition) do
    Holder.set_num_partition(num_partition)
    for order <- 0..num_partition - 1 do
      Holder.set(order)
    end
  end

  @doc """
  Clears all paritions.
  """
  @spec stop :: :ok
  def stop() do
    Holder.clean()
    :ok
  end


  @doc """
  Gets number of partition in cache.
  """
  @spec get_num_partition :: pos_integer
  def get_num_partition() do
    [{_, _, num}] = Ets.lookup(Holder, :num_partition)
    num
  end

  @doc """
  Gets number of online scheduler of VM.
  """
  @spec get_schedulers :: pos_integer
  def get_schedulers() do
    System.schedulers_online()
  end

  @doc """
  Gets hash for data.
  """
  @spec get_hash(any) :: non_neg_integer
  def get_hash(term) do
    :erlang.phash2(term)
  end

  @doc """
  Gets order of partition from data.
  """
  @spec get_pattition_order(any) :: non_neg_integer
  def get_pattition_order(term) do
    num = get_num_partition()
    :erlang.phash2(term, num)
  end
end
