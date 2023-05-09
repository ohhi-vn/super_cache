defmodule SuperCache do
  @moduledoc """
  Documentation for `SuperCache`.
  """
  require Logger

  alias SuperCache.{Partition, Api, Storage}
  alias SuperCache.Partition.{Common}

  def start() do
    opts = [key_pos: 0, partition_pos: 0]
    start(opts)
  end

  def start(opts) do
    Logger.info("start SuperCache with options: #{inspect opts}")
    if Keyword.keyword?(opts) do
      Api.clear_config()
      Enum.each(opts, fn ({key, value}) ->
        Api.set_config(key, value)
      end)

      num_part =
        case Api.get_config(:num_partition, :not_found) do
          :not_found ->
            n = Common.get_schedulers()
            Api.set_config(:num_partition, n)
            n
          n ->
            n
        end

      # start partition handle
      Partition.start(num_part)

      # start to create number of partitions
      Storage.start(num_part)
    else
      {:error, :incorrect_params}
    end
  end

  def stop() do

  end

  def put(data) when is_tuple(data) do
    part_data = Api.get_partition!(data)
    part = Partition.get_partition(part_data)
    Logger.debug("store data (key: #{inspect Api.get_key!(data)}) to partition: #{inspect part}")
    Storage.put(data, part)
  end

  def get_by_key_partition(key, partition) do
    part = Partition.get_partition(partition)
    Logger.debug("get data (key: #{inspect key}) from partition: #{inspect part}")
    Storage.get(key, part)
  end

  def get_same_key_partition(key) do
    get_by_key_partition(key, key)
  end

  def scan_all(pattern) when is_tuple(pattern) do

  end

  def get_by_key_patten(key, pattern)  when is_tuple(pattern) do

  end

  def add_node(node) do

  end
end
