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
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage, Buffer}

  @default_opts [key_pos: 0, partition_pos: 0, cluster: :local, table_type: :set]
  @valid_table_types [:set, :ordered_set, :bag, :duplicate_bag]

  @doc "Start SuperCache with default options (local mode)."
  @spec start!() :: :ok
  def start!(), do: start!(@default_opts)

  @doc "Start SuperCache with the given keyword options. Raises on invalid config."
  @spec start!(keyword) :: :ok
  def start!(opts) do
    Logger.info("super_cache, bootstrap, starting with opts: #{inspect(opts)}")
    validate!(opts)

    case Keyword.get(opts, :cluster, :local) do
      :distributed -> SuperCache.Cluster.Bootstrap.start!(opts)
      :local       -> local_start!(opts)
    end
  end

  @doc "Stop SuperCache and release all ETS resources."
  @spec stop() :: :ok
  def stop() do
    case Config.get_config(:cluster, :local) do
      :distributed -> SuperCache.Cluster.Bootstrap.stop()
      :local       -> local_stop()
    end
  end

  # ── Private — local-mode lifecycle ───────────────────────────────────────────

  defp local_start!(opts) do
    Config.clear_config()
    Enum.each(opts, fn {k, v} -> Config.set_config(k, v) end)

    num = resolve_num_partition()
    resolve_table_type()
    resolve_table_prefix()

    Partition.start(num)
    Storage.start(num)
    Buffer.start(Partition.get_schedulers())

    Config.set_config(:started, true)
    Logger.info("super_cache, bootstrap, ready in local mode (#{num} partitions)")
    :ok
  end

  defp local_stop() do
    Buffer.stop()

    case Config.get_config(:num_partition) do
      nil -> :ok
      n   -> Storage.stop(n)
    end

    Partition.stop()
    Config.set_config(:started, false)
    :ok
  end

  # ── Private — validation ─────────────────────────────────────────────────────

  defp validate!(opts) do
    unless Keyword.keyword?(opts),
      do: raise(ArgumentError, "options must be a keyword list, got: #{inspect(opts)}")

    for key <- [:key_pos, :partition_pos] do
      unless Keyword.has_key?(opts, key),
        do: raise(ArgumentError, "missing required option: #{inspect(key)}")
    end
  end

  # ── Private — config resolution ──────────────────────────────────────────────

  defp resolve_num_partition() do
    case Config.get_config(:num_partition, :not_found) do
      :not_found -> n = Partition.get_schedulers(); Config.set_config(:num_partition, n); n
      n          -> n
    end
  end

  defp resolve_table_type() do
    case Config.get_config(:table_type, :not_found) do
      :not_found                     -> Config.set_config(:table_type, :set)
      t when t in @valid_table_types -> :ok
      bad -> raise ArgumentError, "unsupported table type: #{inspect(bad)}, must be one of #{inspect(@valid_table_types)}"
    end
  end

  defp resolve_table_prefix() do
    case Config.get_config(:table_prefix, :not_found) do
      :not_found -> Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
      _          -> :ok
    end
  end
end
