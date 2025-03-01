defmodule SuperCache do

  @moduledoc """
  SeperCache is a library support for caching data in memory.
  The library is based on Ets table and use some function from :ets for making it easy & friendly to use.
  SuperCache support auto scale based on number of cpu cores and easy to use.
  Type of data is supported is tuple, other type of data can put in a tuple an storage in SuperCache.
  Current version is not support for replicating data to multi nodes in Elixir cluster.
  """

  require Logger

  alias SuperCache.{Partition, Config, Storage}
  alias SuperCache.Internal.Queue, as: LibQueue
  alias SuperCache.Internal.Stream, as: LibStream

  ## API interface ##

  @doc """
  Start SuperCache service (use default config).

  If function is failed, it will raise a error.

  Default config:
    - key_pos: 0
    - partition_pos: 0
    - table_type: :set
    - num_partition: = number of online_schedulers of Erlang VM
  """
  @spec start!() :: :ok
  def start!() do
    opts = [key_pos: 0, partition_pos: 0, cluster: :local, table_type: :bag]
    start!(opts)
  end

  @doc """
  Start SuperCache service with config.

  Config is in Keyword list

  If key_pos or parition_pos is missed, start SuperCache will raise an error.
  """
  @spec start!(list(tuple())) :: :ok
  def start!(opts) do
    Logger.info("super_cache, api, start SuperCache with options: #{inspect opts}")
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "incorrect options"
    end

    unless Keyword.has_key?(opts, :key_pos) do
      raise ArgumentError, "missed key_pos"
    end

    unless Keyword.has_key?(opts, :partition_pos) do
      raise  ArgumentError, "missed partition_pos"
    end

    Config.clear_config()
    for {key, value} <- opts do
      Logger.debug("super_cache, api, add config, key: #{inspect key}, value: #{inspect value}")
      Config.set_config(key, value)
    end

    num_part =
      case Config.get_config(:num_partition, :not_found) do
        :not_found ->
          # set default number of partittion
          n =  Partition.get_schedulers()
          Config.set_config(:num_partition, n)
          n
        n ->
          n
      end

    case Config.get_config(:table_type, :not_found) do
      :not_found ->
        # set default table type
        Config.set_config(:table_type, :set)
      type when type in [:set, :ordered_set, :bag, :duplicate_bag] ->
        :ok
      unsupport ->
        raise ArgumentError, "unsupport table type, #{inspect unsupport}"
    end

    case Config.get_config(:table_prefix, :not_found) do
      :not_found ->
        Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
      _ ->
        :ok
    end

    # start partition handle
    Partition.start(num_part)

    # start to create number of partitions
    Storage.start(num_part)

    # make stream for lazy write.
    stream_fun = fn id ->
      name = String.to_atom("SuperCache.Buffer_#{id}")
      Logger.debug("super_cache, api, starting stream #{inspect name}")

      name
      |> LibQueue.start()
      |> LibStream.create()
      |> LibStream.make_stream_pipe()

      Logger.debug("end stream #{inspect name}")
    end

    for id <- 1..Partition.get_schedulers() do
      spawn( fn ->
        stream_fun.(id)
      end)
    end

    Config.set_config(:started, true)
  end

  @doc """
  Check library is started or not.
  """
  @spec started? :: boolean()
  def started?() do
    Config.get_config(:started, false)
  end

  @doc """
  Init & start library.

  Function uses default config like `start!()`
  """
  @spec start() ::
         :ok | {:error, %{:__exception__ => true, :__struct__ => atom, optional(atom) => any}}
  def start() do
    try do
      start!()
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  @doc """
  Start library with config. Parameters like `start!(opts)`
  """
  @spec start(any) ::
          :ok | {:error, %{:__exception__ => true, :__struct__ => atom, optional(atom) => any}}
  def start(opts) do
    try do
      start!(opts)
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  @doc """
  Clear data & stop library. Call if application no more use SuperCache to free memory.
  """
  @spec stop() :: :ok
  def stop() do
    case Config.get_config(:num_partition) do
      nil ->
        Logger.warning("super_cache, api, something wrong, cannot shutdown success")
      n when is_integer(n) ->
        Storage.stop(n)
    end

    Partition.stop()
    Config.set_config(:started, false)
  end

  @doc """
  Store tuple to cache.

  Tuple size of parameter must be equal or larger than key_pos/partition_pos.

  The function must call after `start!` SuperCache.

  If success the function will return true, other cases it will raise a error.
  """
  @spec put!(tuple()) :: true
  def put!(data) when is_tuple(data) do
    part_data = Config.get_partition!(data)
    part = Partition.get_partition(part_data)
    Logger.debug("super_cache, api, store data (key: #{inspect Config.get_key!(data)}) to partition: #{inspect part}")
    Storage.put(data, part)
  end

  @doc """
  Store tuple to cache. The function works like `put!(data)`
  Result return is true or {:error, reason}
  """
  @spec put(tuple()) :: true | {:error, %{:__exception__ => true, :__struct__ => atom, optional(atom) => any}}
  def put(data) do
    try do
      put!(data)
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  @doc """
  Data is push to buffer before it was written to cache.
  If you don't need read data immediately this will help improve performance.
  """
  @spec lazy_put(tuple) :: any
  def lazy_put(data) when is_tuple(data) do
    id = Enum.random(1..Partition.get_schedulers())
    name = String.to_atom("SuperCache.Buffer_#{id}")
    LibQueue.add(name, data)
  end

  @doc """
  Get data (list of tuple) from cache.
  This is simple way to get data but need key & partition data must same postion with config.
  """
  @spec get!(tuple) :: [tuple]
  def get!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    part_data = Config.get_partition!(data)
    part = Partition.get_partition(part_data)
    Logger.debug("super_cache, api, store data (key: #{inspect key}) to partition: #{inspect part}")
    Storage.get(key, part)
  end

  @doc """
  Same with get!/1 function but doesn's raise error if failed.
  """
  @spec get(tuple) :: [tuple] | {:error, %{:__exception__ => true, :__struct__ => atom, optional(atom) => any}}
  def get(data) when is_tuple(data) do
    try do
      get!(data)
    rescue
      err ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  @doc """
  Get data from cache with key and paritition.
  Function return list of tuple.
  """
  @spec get_by_key_partition!(any, any) :: [tuple]
  def get_by_key_partition!(key, partition) do
    part = Partition.get_partition(partition)
    Logger.debug("super_cache, api, get data (key: #{inspect key}) from partition #{inspect part}")
    Storage.get(key, part)
  end

  @doc """
  Get data from cache in case key_pos & partition in config is same.
  Function return list of tuple.
  """
  @spec get_same_key_partition!(any) :: [tuple]
  def get_same_key_partition!(key) do
    get_by_key_partition!(key, key)
  end


  @doc """
  Gets objects in storage by pattern matching. If partition data is atom :_, it will scan all
  partitions.

  Function works like `:ets.match_pattern/2`
  """
  @spec get_by_match!(any, tuple) :: any
  def get_by_match!(partition_data, pattern) when is_tuple(pattern) do
    partitions =
      case partition_data do
        :_ -> # scan all partitions
        List.flatten(Partition.get_all_partition())
        data -> # scan one partition
          [Partition.get_partition(data)]
      end
    Logger.debug("super_cache, api, get_by_match, list of partition for pattern (#{inspect pattern}): #{inspect partitions})")
    Enum.reduce(partitions, [], fn el, result ->
      Storage.get_by_match(pattern, el) ++ result
    end)
  end

  @doc """
  Scan all storage partitions with pattern.

  Function works like :ets.
  """
  @spec get_by_match!(tuple) :: any
  def get_by_match!(pattern) when is_tuple(pattern) do
    get_by_match!(:_, pattern)
  end

  @doc """
  The function works like `:ets.match_object/2`.
  partition_data is used to get partition. if partition_data = :_ the function will scan all partitions.
  """
  @spec get_by_match_object!(any, tuple) :: any
  def get_by_match_object!(partition_data, pattern) when is_tuple(pattern) do
    partitions =
      case partition_data do
        :_ -> # scan all partitions
        List.flatten(Partition.get_all_partition())
        data -> # scan one partition
          [Partition.get_partition(data)]
      end
    Logger.debug("super_cache, api, get_by_match_object, list of partition for pattern (#{inspect pattern}): #{inspect partitions})")
    Enum.reduce(partitions, [], fn el, result ->
      Storage.get_by_match_object(pattern, el) ++ result
    end)
  end

  @doc """
  Function works like `get_by_match_object!/2`, it will scan all partitions.
  """
  @spec get_by_match_object!(tuple) :: any
  def get_by_match_object!(pattern) when is_tuple(pattern) do
    get_by_match_object!(:_, pattern)
  end

  @doc """
  Function is used to call anonymous function with data(tuple) in a partition.
  acc parameter is used to call function with special options or store data.
  """
  @spec scan!(any, (any, any -> any), any) :: any
  def scan!(partition_data, fun, acc) when is_function(fun, 2) do
    partitions =
      case partition_data do
        :_ -> # scan all partitions
        List.flatten(Partition.get_all_partition())
        data -> # scan one partition
          [Partition.get_partition(data)]
      end
    Logger.debug("super_cache, api, scan, list of partition: #{inspect partitions})")
    Enum.reduce(partitions, acc, fn el, result ->
      Storage.scan(fun, result, el)
    end)
  end

  @doc """
  Function works like `scan!/2` but with all paritions.
  """
  @spec scan!((any, any -> any), any) :: any
  def scan!(fun, acc) when is_function(fun, 2) do
    scan!(:_, fun, acc)
  end

  @doc """
  Detele data in cache with key & partition store same as `put!` function
  """
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data) do
    key = Config.get_key!(data)
    part_data = Config.get_partition!(data)
    Logger.debug("super_cache, api, data used for get partition #{inspect part_data}")
    part = Partition.get_partition(part_data)
    Logger.debug("super_cache, api, store data (key: #{inspect key}) to partition: #{inspect part}")
    Storage.delete(key, part)
    :ok
  end

  @doc """
  Detele all data in cache.
  """
  @spec delete_all() :: :ok
  def delete_all() do
    partitions = List.flatten(Partition.get_all_partition())
    for p <- partitions do
      Storage.delete_all(p)
    end
    :ok
  end

  @doc """
  Delete data by pattern in a partition.
  """
  @spec delete_by_match!(any, tuple) :: any
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    partitions =
      case partition_data do
        :_ -> # scan all partitions
        List.flatten(Partition.get_all_partition())
        data -> # scan one partition
          [Partition.get_partition(data)]
      end
    Logger.debug("super_cache, api, get_by_match_object, list of partition for pattern (#{inspect pattern}): #{inspect partitions})")
    for p <- partitions do
      Storage.delete_match(pattern, p)
    end
  end

  @doc """
  Same as `delete_by_match` but with all partitions in cache.
  """
  @spec delete_by_match!(tuple) :: any
  def delete_by_match!(pattern) when is_tuple(pattern) do
    delete_by_match!(:_, pattern)
  end

  @doc """
  Delete data in a partition by key & data which used to get partition.
  """
  @spec delete_by_key_partition!(any, any) :: true
  def delete_by_key_partition!(key, partition_data) do
    part = Partition.get_partition(partition_data)
    Logger.debug("super_cache, api, get data (key: #{inspect key}) from partition #{inspect part}")
    Storage.delete(key, part)
  end

  @doc """
  Delete data in a partition by key & data which used to get partition is key.
  """
  @spec delete_same_key_partition!(any) :: true
  def delete_same_key_partition!(key) do
    delete_by_key_partition!(key, key)
  end

  @doc """
  Get stats in all tables
  """
  def stats() do
    partitions = List.flatten(Partition.get_all_partition())
    list =
      for p <- partitions do
        Storage.stats(p)
      end
    total = Enum.reduce(list, 0, fn {_, size}, acc -> acc + size end)
    list ++ [total: total]
  end
end
