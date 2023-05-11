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
      raise ArgumentError, "incorrect options"
    end

    unless Keyword.has_key?(opts, :key_pos) do
      raise ArgumentError, "missed key_pos"
    end

    unless Keyword.has_key?(opts, :partition_pos) do
      raise  ArgumentError, "missed partition_pos"
    end

    Api.clear_config()
    Enum.each(opts, fn ({key, value}) ->
      Logger.debug("add config, key: #{inspect key}, value: #{inspect value}")
      Api.set_config(key, value)
    end)

    num_part =
      case Api.get_config(:num_partition, :not_found) do
        :not_found ->
          # set default number of partittion
          n = Common.get_schedulers()
          Api.set_config(:num_partition, n)
          n
        n ->
          n
      end

    case Api.get_config(:table_type, :not_found) do
      :not_found ->
        # set default table type
        Api.set_config(:table_type, :set)
      type when type in [:set, :ordered_set, :bag, :duplicate_bag] ->
        :ok
      unsupport ->
        raise ArgumentError, "unsupport table type, #{inspect unsupport}"
    end

    # start partition handle
    Partition.start(num_part)

    # start to create number of partitions
    Storage.start(num_part)

  end

  def start() do
    try do
      start!()
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  def start(opts) do
    try do
      start!(opts)
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
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

  @spec get_same_key_partition(any) :: [tuple]
  def get_same_key_partition(key) do
    get_by_key_partition(key, key)
  end

  @doc """
  Gets objects in storage by pattern matching. If the pos of partition is atom :_, it will scan all
  partitions
  """
  def get_by_pattern(pattern) when is_tuple(pattern) do
    partitions =
      case Api.get_partition!(pattern) do
        :_ -> # scan all partitions
        List.flatten(Partition.get_all_partition())
        data -> # scan one partition
          [Partition.get_partition(data)]
      end
    Logger.debug("list of partition for pattern (#{inspect pattern}): #{inspect partitions})")
    Enum.reduce(partitions, [], fn el, result ->
      Storage.get_by_pattern(pattern, el) ++ result
    end)
  end

  def scan_all(fun) do

  end

  def add_node(node) do
    :ok
  end
end
