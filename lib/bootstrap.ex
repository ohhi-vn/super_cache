defmodule SuperCache.Bootstrap do
  @moduledoc """
  Orchestrates the startup and shutdown sequence for SuperCache.

  Extracted from the top-level `SuperCache` module so that each concern
  (config validation, partition setup, storage setup, buffer setup) is
  handled in one place and easy to test in isolation.
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage, Buffer}

  @default_opts [key_pos: 0, partition_pos: 0, cluster: :local, table_type: :set]

  @valid_table_types [:set, :ordered_set, :bag, :duplicate_bag]

  ## API ##

  @doc """
  Start SuperCache with default options.  Raises on invalid config.
  """
  @spec start!() :: :ok
  def start!(), do: start!(@default_opts)

  @doc """
  Start SuperCache with caller-supplied keyword options.  Raises on invalid config.

  Required keys: `:key_pos`, `:partition_pos`.
  Optional keys: `:num_partition`, `:table_type`, `:table_prefix`.
  """
  @spec start!(keyword) :: :ok
  def start!(opts) do
    Logger.info("super_cache, bootstrap, starting with options: #{inspect(opts)}")

    validate!(opts)

    Config.clear_config()
    Enum.each(opts, fn {k, v} -> Config.set_config(k, v) end)

    num_partition = resolve_num_partition()
    resolve_table_type()
    resolve_table_prefix()

    Partition.start(num_partition)
    Storage.start(num_partition)
    Buffer.start(Partition.get_schedulers())

    Config.set_config(:started, true)
    Logger.info("super_cache, bootstrap, started (#{num_partition} partitions)")
    :ok
  end

  @doc """
  Stop SuperCache: clear storage, partitions, buffers and reset config flag.
  """
  @spec stop() :: :ok
  def stop() do
    Buffer.stop()

    case Config.get_config(:num_partition) do
      nil ->
        Logger.warning("super_cache, bootstrap, stop called but :num_partition not set")

      n when is_integer(n) ->
        Storage.stop(n)
    end

    Partition.stop()
    Config.set_config(:started, false)
    :ok
  end

  ## Private — validation ##

  defp validate!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "options must be a keyword list, got: #{inspect(opts)}"
    end

    for required <- [:key_pos, :partition_pos] do
      unless Keyword.has_key?(opts, required) do
        raise ArgumentError, "missing required option: #{inspect(required)}"
      end
    end
  end

  ## Private — config resolution with defaults ##

  defp resolve_num_partition() do
    case Config.get_config(:num_partition, :not_found) do
      :not_found ->
        n = Partition.get_schedulers()
        Config.set_config(:num_partition, n)
        n

      n ->
        n
    end
  end

  defp resolve_table_type() do
    case Config.get_config(:table_type, :not_found) do
      :not_found ->
        Config.set_config(:table_type, :set)

      type when type in @valid_table_types ->
        :ok

      bad ->
        raise ArgumentError, "unsupported table type: #{inspect(bad)}, must be one of #{inspect(@valid_table_types)}"
    end
  end

  defp resolve_table_prefix() do
    case Config.get_config(:table_prefix, :not_found) do
      :not_found -> Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
      _ -> :ok
    end
  end
end
