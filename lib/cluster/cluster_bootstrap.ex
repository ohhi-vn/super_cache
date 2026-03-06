
defmodule SuperCache.Cluster.Bootstrap do
  @moduledoc """
  Cluster-aware variant of `SuperCache.Bootstrap`.

  Extra options:
    - `:replication_factor` — total copies of each partition (primary +
      replicas).  Default `2`.  Set to `1` to disable replication.
    - `:cluster` — `:local` (default) or `:distributed`.
  """
  require Logger

  alias SuperCache.{Config, Partition, Storage, Buffer}
  alias SuperCache.Cluster.Manager, as: ClusterManager

  @default_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    table_type:         :set
  ]

  @valid_table_types [:set, :ordered_set, :bag, :duplicate_bag]

  @spec start!(keyword) :: :ok
  def start!(opts \\ @default_opts) do
    Logger.info("super_cache, cluster.bootstrap, starting with opts: #{inspect(opts)}")
    validate!(opts)

    Config.clear_config()
    Enum.each(opts, fn {k, v} -> Config.set_config(k, v) end)

    num_partition = resolve_num_partition()
    resolve_table_type()
    resolve_table_prefix()

    Partition.start(num_partition)
    Storage.start(num_partition)
    Buffer.start(Partition.get_schedulers())

    if Config.get_config(:cluster) == :distributed do
      # Trigger a full sync from existing nodes if any are already up.
      live = ClusterManager.live_nodes() -- [node()]
      if live != [] do
        Logger.info("super_cache, cluster.bootstrap, requesting sync from #{inspect(live)}")
        ClusterManager.full_sync()
      end
    end

    Config.set_config(:started, true)
    Logger.info("super_cache, cluster.bootstrap, ready (#{num_partition} partitions)")
    :ok
  end

  @spec stop() :: :ok
  def stop() do
    Buffer.stop()

    case Config.get_config(:num_partition) do
      nil -> :ok
      n   -> Storage.stop(n)
    end

    Partition.stop()
    Config.set_config(:started, false)
    :ok
  end

  ## Private ##

  defp validate!(opts) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    for key <- [:key_pos, :partition_pos] do
      unless Keyword.has_key?(opts, key), do: raise(ArgumentError, "missing required option: #{inspect(key)}")
    end
  end

  defp resolve_num_partition() do
    case Config.get_config(:num_partition, :not_found) do
      :not_found ->
        n = Partition.get_schedulers()
        Config.set_config(:num_partition, n)
        n
      n -> n
    end
  end

  defp resolve_table_type() do
    case Config.get_config(:table_type, :not_found) do
      :not_found                        -> Config.set_config(:table_type, :set)
      t when t in @valid_table_types    -> :ok
      bad -> raise ArgumentError, "unsupported table type: #{inspect(bad)}"
    end
  end

  defp resolve_table_prefix() do
    case Config.get_config(:table_prefix, :not_found) do
      :not_found -> Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
      _          -> :ok
    end
  end
end
