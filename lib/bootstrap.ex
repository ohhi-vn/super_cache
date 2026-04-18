defmodule SuperCache.Bootstrap do
  @moduledoc """
  Unified startup and shutdown for SuperCache.

  Handles both `:local` (single-node) and `:distributed` (cluster) modes.
  The mode is selected via the `:cluster` option:

  - `:local` (default) — single-node ETS cache, no replication.
  - `:distributed` — cluster-aware; delegates to `SuperCache.Cluster.Bootstrap`.

  ## Options

  | Option              | Type    | Default         | Description                          |
  |---------------------|---------|-----------------|--------------------------------------|
  | `:key_pos`          | integer | `0`             | Tuple index used as the ETS key      |
  | `:partition_pos`    | integer | `0`             | Tuple index used to select partition |
  | `:cluster`          | atom    | `:local`        | `:local` or `:distributed`           |
  | `:num_partition`    | integer | scheduler count | Number of ETS partitions             |
  | `:table_type`       | atom    | `:set`          | ETS table type                       |
  | `:table_prefix`     | string  | `"SuperCache…"` | Prefix for ETS table atom names      |

  Additional options for `:distributed` mode (see `SuperCache.Cluster.Bootstrap`):

  | Option               | Type    | Default  | Description                        |
  |----------------------|---------|----------|------------------------------------|
  | `:replication_factor`| integer | `2`      | Total copies (primary + replicas)  |
  | `:replication_mode`  | atom    | `:async` | `:async`, `:sync`, or `:strong`    |

  ## Examples

  Start with defaults (local mode):

      SuperCache.Bootstrap.start!()

  Start with custom configuration:

      opts = [
        key_pos: 0,
        partition_pos: 1,
        num_partition: 8,
        table_type: :bag
      ]
      SuperCache.Bootstrap.start!(opts)

  Stop the cache and release all resources:

      SuperCache.Bootstrap.stop()
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Config, Partition, Storage, Buffer}

  @default_opts [key_pos: 0, partition_pos: 0, cluster: :local, table_type: :set]
  @valid_table_types [:set, :ordered_set, :bag, :duplicate_bag]

  @doc """
  Start SuperCache with default options (local mode).

  Uses `key_pos: 0`, `partition_pos: 0`, `cluster: :local`, and `table_type: :set`.
  The number of partitions defaults to the number of online schedulers.

  ## Examples

      SuperCache.Bootstrap.start!()
      # => :ok
  """
  @spec start!() :: :ok
  def start!(), do: start!(@default_opts)

  @doc """
  Start SuperCache with the given keyword options.

  Validates all options before starting. Raises `ArgumentError` if any
  option is invalid or missing.

  ## Options

  See module documentation for the full list of supported options.

  ## Examples

      SuperCache.Bootstrap.start!(key_pos: 0, partition_pos: 1, num_partition: 4)
      # => :ok

      SuperCache.Bootstrap.start!(cluster: :invalid)
      # => ** (ArgumentError) unsupported cluster mode: :invalid
  """
  @spec start!(keyword) :: :ok
  def start!(opts) when is_list(opts) do
    t0 = System.monotonic_time(:millisecond)

    Logger.info("super_cache, bootstrap, starting with options: #{inspect(opts)}")

    validate!(opts)

    result =
      case Keyword.get(opts, :cluster, :local) do
        :distributed ->
          Logger.info("super_cache, bootstrap, starting in distributed mode")
          SuperCache.Cluster.Bootstrap.start!(opts)

        :local ->
          Logger.info("super_cache, bootstrap, starting in local mode")
          local_start!(opts)

        other ->
          raise ArgumentError,
                "unsupported cluster mode: #{inspect(other)}, expected :local or :distributed"
      end

    elapsed = System.monotonic_time(:millisecond) - t0
    Logger.info("super_cache, bootstrap, started successfully in #{elapsed}ms")
    result
  end

  def start!(opts) do
    raise ArgumentError, "options must be a keyword list, got: #{inspect(opts)}"
  end

  @doc """
  Stop SuperCache and release all ETS resources.

  Gracefully shuts down buffers, storage partitions, and the partition
  registry. Configuration is preserved so the cache can be restarted
  with the same settings.

  ## Examples

      SuperCache.Bootstrap.stop()
      # => :ok
  """
  @spec stop() :: :ok
  def stop() do
    Logger.info("super_cache, bootstrap, stopping...")
    t0 = System.monotonic_time(:millisecond)

    result =
      case Config.get_config(:cluster, :local) do
        :distributed ->
          Logger.info("super_cache, bootstrap, stopping distributed mode")
          SuperCache.Cluster.Bootstrap.stop()

        :local ->
          local_stop()
      end

    elapsed = System.monotonic_time(:millisecond) - t0
    Logger.info("super_cache, bootstrap, stopped successfully in #{elapsed}ms")
    result
  end

  # ── Private — local-mode lifecycle ───────────────────────────────────────────

  defp local_start!(opts) do
    # Clear previous configuration to avoid stale state
    Config.clear_config()

    # Apply new configuration
    Enum.each(opts, fn {k, v} -> Config.set_config(k, v) end)

    # Resolve defaults for missing options
    num = resolve_num_partition()
    resolve_table_type()
    resolve_table_prefix()

    # Start components in dependency order
    with :ok <- start_partitions(num),
         :ok <- start_storage(num),
         :ok <- start_buffers() do
      Config.set_config(:started, true)

      Logger.info(
        "super_cache, bootstrap, local mode ready (#{num} partition(s), table_type: #{Config.get_config(:table_type)})"
      )

      :ok
    else
      {:error, reason} ->
        Logger.error("super_cache, bootstrap, failed to start local mode: #{inspect(reason)}")
        # Rollback on failure
        local_stop()
        raise RuntimeError, "failed to start SuperCache: #{inspect(reason)}"
    end
  end

  defp local_stop() do
    Logger.info("super_cache, bootstrap, stopping local mode components...")

    # Stop in reverse dependency order
    Buffer.stop()
    SuperCache.Log.debug(fn -> "super_cache, bootstrap, buffers stopped" end)

    case Config.get_config(:num_partition) do
      nil ->
        SuperCache.Log.debug(fn -> "super_cache, bootstrap, no partitions to stop" end)
        :ok

      n when is_integer(n) and n > 0 ->
        Storage.stop(n)

        SuperCache.Log.debug(fn ->
          "super_cache, bootstrap, #{n} storage partition(s) stopped"
        end)

      other ->
        Logger.warning("super_cache, bootstrap, invalid num_partition config: #{inspect(other)}")
        :ok
    end

    Partition.stop()
    SuperCache.Log.debug(fn -> "super_cache, bootstrap, partition registry cleared" end)

    Config.set_config(:started, false)
    Logger.info("super_cache, bootstrap, local mode stopped")
    :ok
  end

  # ── Private — component startup ──────────────────────────────────────────────

  defp start_partitions(num) do
    SuperCache.Log.debug(fn -> "super_cache, bootstrap, creating #{num} partition(s)" end)

    try do
      Partition.start(num)
      :ok
    rescue
      err ->
        Logger.error("super_cache, bootstrap, failed to create partitions: #{inspect(err)}")
        {:error, {:partition_start_failed, err}}
    end
  end

  defp start_storage(num) do
    SuperCache.Log.debug(fn -> "super_cache, bootstrap, creating #{num} storage table(s)" end)

    try do
      Storage.start(num)
      :ok
    rescue
      err ->
        Logger.error("super_cache, bootstrap, failed to create storage tables: #{inspect(err)}")
        {:error, {:storage_start_failed, err}}
    end
  end

  defp start_buffers() do
    num_schedulers = Partition.get_schedulers()
    SuperCache.Log.debug(fn -> "super_cache, bootstrap, creating #{num_schedulers} buffer(s)" end)

    try do
      Buffer.start(num_schedulers)
      :ok
    rescue
      err ->
        Logger.error("super_cache, bootstrap, failed to create buffers: #{inspect(err)}")
        {:error, {:buffer_start_failed, err}}
    end
  end

  # ── Private — validation ─────────────────────────────────────────────────────

  defp validate!(opts) do
    unless Keyword.keyword?(opts),
      do: raise(ArgumentError, "options must be a keyword list, got: #{inspect(opts)}")

    # Validate required options
    for key <- [:key_pos, :partition_pos] do
      unless Keyword.has_key?(opts, key),
        do: raise(ArgumentError, "missing required option: #{inspect(key)}")

      value = Keyword.fetch!(opts, key)

      unless is_integer(value) and value >= 0,
        do:
          raise(
            ArgumentError,
            "#{inspect(key)} must be a non-negative integer, got: #{inspect(value)}"
          )
    end

    # Validate cluster mode
    case Keyword.get(opts, :cluster, :local) do
      mode when mode in [:local, :distributed] ->
        :ok

      other ->
        raise ArgumentError,
              "unsupported cluster mode: #{inspect(other)}, expected :local or :distributed"
    end

    # Validate table type if provided
    case Keyword.get(opts, :table_type) do
      nil ->
        :ok

      type when type in @valid_table_types ->
        :ok

      other ->
        raise ArgumentError,
              "unsupported table_type: #{inspect(other)}, expected one of #{inspect(@valid_table_types)}"
    end

    # Validate num_partition if provided
    case Keyword.get(opts, :num_partition) do
      nil ->
        :ok

      n when is_integer(n) and n > 0 ->
        :ok

      other ->
        raise ArgumentError, "num_partition must be a positive integer, got: #{inspect(other)}"
    end

    # Validate replication_factor if provided
    case Keyword.get(opts, :replication_factor) do
      nil ->
        :ok

      n when is_integer(n) and n >= 1 ->
        :ok

      other ->
        raise ArgumentError,
              "replication_factor must be a positive integer, got: #{inspect(other)}"
    end

    # Validate replication_mode if provided
    case Keyword.get(opts, :replication_mode) do
      nil ->
        :ok

      mode when mode in [:async, :sync, :strong] ->
        :ok

      other ->
        raise ArgumentError,
              "unsupported replication_mode: #{inspect(other)}, expected :async, :sync, or :strong"
    end

    # Cross-validation: replication_factor should not exceed reasonable bounds
    case {Keyword.get(opts, :replication_factor), Keyword.get(opts, :num_partition)} do
      {rf, np} when is_integer(rf) and is_integer(np) and rf > np ->
        Logger.warning(
          "super_cache, bootstrap, replication_factor (#{rf}) exceeds num_partition (#{np}). " <>
            "This may reduce effective replication."
        )

        :ok

      _ ->
        :ok
    end

    :ok
  end

  # ── Private — config resolution ──────────────────────────────────────────────

  defp resolve_num_partition() do
    case Config.get_config(:num_partition, :not_found) do
      :not_found ->
        n = Partition.get_schedulers()
        Config.set_config(:num_partition, n)
        Logger.info("super_cache, bootstrap, num_partition defaulted to #{n} (scheduler count)")
        n

      n when is_integer(n) and n > 0 ->
        Logger.info("super_cache, bootstrap, using configured num_partition: #{n}")
        n

      other ->
        raise ArgumentError, "invalid num_partition: #{inspect(other)}"
    end
  end

  defp resolve_table_type() do
    case Config.get_config(:table_type, :not_found) do
      :not_found ->
        Config.set_config(:table_type, :set)
        SuperCache.Log.debug(fn -> "super_cache, bootstrap, table_type defaulted to :set" end)

      t when t in @valid_table_types ->
        SuperCache.Log.debug(fn -> "super_cache, bootstrap, using table_type: #{inspect(t)}" end)
        :ok

      bad ->
        raise ArgumentError,
              "unsupported table_type: #{inspect(bad)}, must be one of #{inspect(@valid_table_types)}"
    end
  end

  defp resolve_table_prefix() do
    case Config.get_config(:table_prefix, :not_found) do
      :not_found ->
        Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
        SuperCache.Log.debug(fn -> "super_cache, bootstrap, table_prefix defaulted" end)

      prefix when is_binary(prefix) ->
        SuperCache.Log.debug(fn ->
          "super_cache, bootstrap, using table_prefix: #{inspect(prefix)}"
        end)

        :ok

      other ->
        raise ArgumentError, "table_prefix must be a binary string, got: #{inspect(other)}"
    end
  end
end
