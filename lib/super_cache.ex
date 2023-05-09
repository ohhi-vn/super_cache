defmodule SuperCache do
  @moduledoc """
  Documentation for `SuperCache`.
  """
  require Logger

  alias SuperCache.{Partition, Api, Storage}
  alias SuperCache.Partition.{Common}

  def start!() do
    opts = [key_pos: 0, partition_pos: 0]
    start!(opts)
  end

  def start!(opts) do
    Logger.info("start SuperCache with options: #{inspect opts}")
    unless Keyword.keyword?(opts) do
      raise "incorrect options"
    end

    unless Keyword.has_key?(opts, :key_pos) do
      raise "missed key_pos"
    end

    unless Keyword.has_key?(opts, :partition_pos) do
      raise  "missed partition_pos"
    end

    Api.clear_config()
    Enum.each(opts, fn ({key, value}) ->
      Logger.debug("add config, key: #{inspect key}, value: #{inspect value}")
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

  end

  def stop() do
    case Api.get_config(:num_partition) do
      nil ->
        Logger.warn("something wrong, cannot shutdown success")
      n when is_integer(n) ->
        Storage.stop(n)
    end

    Partition.stop()
  end

  def put(data) when is_tuple(data) do
    part_data = Api.get_partition!(data)
    Logger.debug("data used for get partition #{inspect part_data}")
    part = Partition.get_partition(part_data)
    Logger.debug("store data (key: #{inspect Api.get_key!(data)}) to partition: #{inspect part}")
    Storage.put(data, part)
  end

  def get(data) when is_tuple(data) do
    key = Api.get_key!(data)
    part_data = Api.get_partition!(data)
    Logger.debug("data used for get partition #{inspect part_data}")
    part = Partition.get_partition(part_data)
    Logger.debug("store data (key: #{inspect key}) to partition: #{inspect part}")
    Storage.get(key, part)
  end

  def get_by_key_partition(key, partition) do
    part = Partition.get_partition(partition)
    Logger.debug("get data (key: #{inspect key}) from partition #{inspect part}")
    Storage.get(key, part)
  end

  def get_same_key_partition(key) do
    get_by_key_partition(key, key)
  end

  def scan_all(pattern) when is_tuple(pattern) do

  end

  def add_node(node) do

  end
end
