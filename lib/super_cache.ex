defmodule SuperCache do
  @moduledoc """
  Documentation for `SuperCache`.
  """

  alias SuperCache.{Partition, Common}

  def start(%{} = opts) do

  end

  def stop() do

  end

  def get_by_key(key) do
    range = Common.get_range()
    partition = Partition.Utils.get_pattition(key, range)
  end

  def scan_all(pattern) when is_tuple(pattern) do

  end

  def get_by_key_patten(key, pattern)  when is_tuple(pattern) do

  end

  def add_node(node) do

  end
end
