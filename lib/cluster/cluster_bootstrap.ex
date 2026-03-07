defmodule SuperCache.Cluster.Bootstrap do
  @moduledoc """
  Cluster-aware startup and shutdown for SuperCache.

  ## Start options

  All options accepted by `SuperCache.Bootstrap.start!/1` are valid here,
  plus the following cluster-specific keys:

  | Option               | Type      | Default        | Description                              |
  |----------------------|-----------|----------------|------------------------------------------|
  | `:cluster`           | atom      | `:distributed` | `:local` or `:distributed`               |
  | `:replication_factor`| integer   | `2`            | Total copies (primary + replicas)        |
  | `:replication_mode`  | atom      | `:async`       | `:async`, `:sync`, or `:strong` (3PC)    |
  | `:num_partition`     | integer   | scheduler count| Number of ETS partitions                 |
  | `:table_type`        | atom      | `:set`         | ETS table type                           |

  ## Replication modes

  - **`:async`** — fire-and-forget. Lowest latency; eventual consistency.
  - **`:sync`**  — synchronous delivery to all replicas before returning.
    One extra RTT per write.
  - **`:strong`** — three-phase commit via
    `SuperCache.Cluster.ThreePhaseCommit`.  Guarantees that either all
    replicas apply a write or none do.  Three extra RTTs per write.

  ## Start sequence

  1. Validate options.
  2. Write all options to `SuperCache.Config`.
  3. Start `Partition` and `Storage` subsystems.
  4. Start the `Buffer` write-buffer pool.
  5. If `:replication_mode` is `:strong`, run crash-recovery via
     `ThreePhaseCommit.recover/0` to resolve in-doubt transactions left
     over from a previous crash.
  6. If other nodes are already live, request a full sync so this node
     receives a consistent snapshot of each partition.
  7. Mark `:started` in config.

  ## Stop sequence

  1. Stop the `Buffer` (flushes pending lazy writes).
  2. Stop `Storage` (deletes ETS tables).
  3. Stop `Partition` (clears partition registry).
  4. Mark `:started` as `false`.
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage, Buffer}
  alias SuperCache.Cluster.{Manager, ThreePhaseCommit}

  @default_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    replication_mode:   :async,
    table_type:         :set
  ]

  @valid_table_types  [:set, :ordered_set, :bag, :duplicate_bag]
  @valid_rep_modes    [:async, :sync, :strong]

  # ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Start SuperCache in cluster mode with the given options.

  Raises `ArgumentError` for invalid options.
  """
  @spec start!(keyword) :: :ok
  def start!(opts \\ @default_opts) do
    Logger.info("super_cache, cluster.bootstrap, starting with opts: #{inspect(opts)}")

    validate!(opts)

    Config.clear_config()
    Enum.each(opts, fn {k, v} -> Config.set_config(k, v) end)

    num_partition = resolve_num_partition()
    resolve_table_type()
    resolve_table_prefix()
    resolve_replication_mode()

    Partition.start(num_partition)
    Storage.start(num_partition)
    Buffer.start(Partition.get_schedulers())

    # Resolve any in-doubt 3PC transactions from a previous crash.
    if Config.get_config(:replication_mode) == :strong do
      Logger.info("super_cache, cluster.bootstrap, running 3PC crash recovery")
      ThreePhaseCommit.recover()
    end

    # Pull a consistent snapshot from existing cluster members.
    if Config.get_config(:cluster) == :distributed do
      live = Manager.live_nodes() -- [node()]

      if live != [] do
        Logger.info(
          "super_cache, cluster.bootstrap, requesting full sync from: #{inspect(live)}"
        )
        Manager.full_sync()
      end
    end

    Config.set_config(:started, true)
    Logger.info("super_cache, cluster.bootstrap, ready (#{num_partition} partitions)")
    :ok
  end

  @doc """
  Stop SuperCache and release all ETS resources.
  """
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

  @doc """
  Returns `true` when this node is running in distributed mode and has
  completed start-up.

  Called remotely by `Manager.node_running?/1` via `:erpc`.
  """
  @spec running?() :: boolean
  def running?() do
    Config.get_config(:cluster) == :distributed and
      Config.get_config(:started, false) == true
  end

  # ── Private — validation ─────────────────────────────────────────────────────

  defp validate!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "options must be a keyword list"
    end

    for key <- [:key_pos, :partition_pos] do
      unless Keyword.has_key?(opts, key) do
        raise ArgumentError, "missing required option: #{inspect(key)}"
      end
    end

    if mode = Keyword.get(opts, :replication_mode) do
      unless mode in @valid_rep_modes do
        raise ArgumentError,
          "invalid :replication_mode #{inspect(mode)}, must be one of #{inspect(@valid_rep_modes)}"
      end
    end
  end

  # ── Private — config resolution ──────────────────────────────────────────────

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
      :not_found                     -> Config.set_config(:table_type, :set)
      t when t in @valid_table_types -> :ok
      bad -> raise ArgumentError, "unsupported table type: #{inspect(bad)}"
    end
  end

  defp resolve_table_prefix() do
    case Config.get_config(:table_prefix, :not_found) do
      :not_found -> Config.set_config(:table_prefix, "SuperCache.Storage.Ets")
      _          -> :ok
    end
  end

  defp resolve_replication_mode() do
    case Config.get_config(:replication_mode, :not_found) do
      :not_found -> Config.set_config(:replication_mode, :async)
      m when m in @valid_rep_modes -> :ok
      bad -> raise ArgumentError, "unsupported replication_mode: #{inspect(bad)}"
    end
  end
end
