defmodule SuperCache.Cluster.Bootstrap do
  @moduledoc """
  Cluster-aware startup and shutdown for SuperCache.

  ## Start options

  All options accepted by `SuperCache.Bootstrap.start!/1` are valid here,
  plus the following cluster-specific keys:

  | Option               | Type                     | Default        | Description                              |
  |----------------------|--------------------------|----------------|------------------------------------------|
  | `:cluster`           | atom                     | `:distributed` | `:local` or `:distributed`               |
  | `:replication_factor`| integer                  | `2`            | Total copies (primary + replicas)        |
  | `:replication_mode`  | atom                     | `:async`       | `:async`, `:sync`, or `:strong` (3PC)    |
  | `:num_partition`     | integer                  | scheduler count| Number of ETS partitions                 |
  | `:table_type`        | atom                     | `:set`         | ETS table type                           |

  ## Node-source options (forwarded to `NodeMonitor`)

  | Option         | Type                     | Default | Description                                    |
  |----------------|--------------------------|---------|------------------------------------------------|
  | `:nodes`       | `[node()]`               | —       | Static peer list evaluated once at start-up.   |
  | `:nodes_mfa`   | `{module, atom, [term]}` | —       | Called at init and on every `:refresh_ms` tick.|
  | `:refresh_ms`  | pos_integer              | `5_000` | MFA re-evaluation interval (ignored otherwise).|

  When neither `:nodes` nor `:nodes_mfa` is supplied, `NodeMonitor` falls
  back to watching **all** Erlang-connected nodes (legacy behaviour).

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
  3. Reconfigure `NodeMonitor` with the node-source opts (`:nodes`,
     `:nodes_mfa`, `:refresh_ms`).  This is done early so `Manager` sees
     the correct managed set when it health-checks peers in later steps.
  4. If other nodes are already live, verify that every structural config
     key on this node matches the cluster.  Raises `ArgumentError` on any
     mismatch.  **No ETS tables have been created at this point**, so a
     rejection leaves the node in a completely clean state and `start!/1`
     can be retried with corrected opts without hitting "table already exists".
  5. Start `Partition` and `Storage` subsystems.
  6. Start the `Buffer` write-buffer pool.
  7. If `:replication_mode` is `:strong`, run crash-recovery via
     `ThreePhaseCommit.recover/0` to resolve in-doubt transactions left
     over from a previous crash.
  8. If other nodes are already live, request a full sync so this node
     receives a consistent snapshot of each partition.
  9. Mark `:started` in config.

  ## Stop sequence

  1. Stop the `Buffer` (flushes pending lazy writes).
  2. Stop `Storage` (deletes ETS tables).
  3. Stop `Partition` (clears partition registry).
  4. Mark `:started` as `false`.

  ## Config verification

  When a node joins a running cluster, `start!/1` calls `verify_cluster_config!/1`
  which performs a pairwise comparison of every structural config key against all
  live peers via `:erpc`.  The keys checked are:

  `[:key_pos, :partition_pos, :num_partition, :table_type, :replication_factor, :replication_mode]`

  `:started`, `:cluster`, and `:table_prefix` are intentionally excluded:
  `:started` is a liveness flag that will differ during bootstrap; `:cluster`
  is always `:distributed` in this module; `:table_prefix` must already match
  for ETS tables to be addressable so a mismatch would cause an earlier crash.

  Any mismatch raises `ArgumentError` listing every divergent key with both
  the local and remote values so the operator can identify the problem
  immediately rather than observing silent data inconsistency later.
  """

  require Logger

  alias SuperCache.{Config, Partition, Storage, Buffer}
  alias SuperCache.Cluster.{Manager, NodeMonitor, ThreePhaseCommit}

  @default_opts [
    key_pos:            0,
    partition_pos:      0,
    cluster:            :distributed,
    replication_factor: 2,
    replication_mode:   :async,
    table_type:         :set
  ]

  @valid_table_types [:set, :ordered_set, :bag, :duplicate_bag]
  @valid_rep_modes   [:async, :sync, :strong]

  # Keys forwarded verbatim to NodeMonitor.reconfigure/1.
  # All three are optional; omitting them preserves NodeMonitor's current
  # source (or its :all fallback on a fresh start).
  @node_source_keys [:nodes, :nodes_mfa, :refresh_ms]

  # Structural keys that MUST be identical on every cluster node.
  @config_keys [
    :key_pos,
    :partition_pos,
    :num_partition,
    :table_type,
    :replication_factor,
    :replication_mode
  ]


  # ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Start SuperCache in cluster mode with the given options.

  Raises `ArgumentError` for invalid options or when the node's structural
  config does not match an already-running cluster.
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

    # Reconfigure NodeMonitor with the node-source opts supplied by the caller.
    #
    # Ordering rationale:
    #   NodeMonitor must know which peers to watch before Manager starts
    #   health-checking them.  Reconfiguring here — after Config is written but
    #   before any subsystem starts — ensures the managed set is correct when
    #   Manager.live_nodes/0 is called in the verify and full-sync steps below.
    #
    #   If the caller omits all node-source keys, node_source_opts/1 returns [],
    #   and NodeMonitor.reconfigure([]) falls back to :all mode (watches every
    #   Erlang-connected node), preserving legacy behaviour.
    reconfigure_node_monitor(opts)

    # Verify cluster config BEFORE starting any subsystem.
    #
    # Ordering rationale:
    #   verify_cluster_config!/1 only needs the config keys that were just
    #   written to SuperCache.Config — it performs no ETS operations and has
    #   no dependency on Partition, Storage, or Buffer.
    #
    #   Moving verification here means that if the check fails (ArgumentError),
    #   no ETS tables have been created yet.  The node is left in a clean state
    #   and start!/1 can be retried with corrected opts without hitting
    #   "table name already exists" from a prior partial start.
    if Config.get_config(:cluster) == :distributed do
      live = Manager.live_nodes() -- [node()]

      if live != [] do
        verify_cluster_config!(live)
      end
    end

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

  @doc """
  Return the structural config of this node as a map.

  Called via `:erpc` from a joining node during config verification.
  Returns only the keys in `@config_keys` — never liveness flags.

  ## Example

      SuperCache.Cluster.Bootstrap.export_config()
      # => %{key_pos: 0, partition_pos: 0, num_partition: 8,
      #       table_type: :set, replication_factor: 2, replication_mode: :async}
  """
  @spec export_config() :: map
  def export_config() do
    Map.new(@config_keys, fn k -> {k, Config.get_config(k)} end)
  end

  @doc """
  Return the full partition map for this node as a list of
  `{partition_idx, {primary, replicas}}` pairs.

  Called via `:erpc` from test helpers on the test node to read the
  partition assignment of a remote peer without crossing the no-lambda
  boundary.  `num` must match `SuperCache.Config.get_config(:num_partition)`
  on the calling node; callers should read that value locally and pass it
  as an argument so the comparison is always against the same reference.

  ## Example

      SuperCache.Cluster.Bootstrap.fetch_partition_map(8)
      # => [{0, {:"a@host", [:"b@host"]}}, ...]
  """
  @spec fetch_partition_map(pos_integer) :: [{non_neg_integer, {node, [node]}}]
  def fetch_partition_map(num) do
    Enum.map(0..(num - 1), fn idx ->
      {idx, SuperCache.Cluster.Manager.get_replicas(idx)}
    end)
  end

  # ── Private — NodeMonitor reconfiguration ────────────────────────────────────

  # Extract the node-source keys the caller passed and forward them to
  # NodeMonitor.  If none were supplied, opts is [], which makes NodeMonitor
  # fall back to :all mode — identical to the behaviour before this feature.
  defp reconfigure_node_monitor(opts) do
    node_opts = Keyword.take(opts, @node_source_keys)

    Logger.info(
      "super_cache, cluster.bootstrap, reconfiguring node_monitor with: #{inspect(node_opts)}"
    )

    NodeMonitor.reconfigure(node_opts)
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

    # Validate node-source options eagerly so errors surface at the call site
    # rather than inside NodeMonitor's GenServer init.
    validate_node_source!(opts)
  end

  # Mirrors the resolve_source/1 guards in NodeMonitor so failures are raised
  # in the caller's process with a clear Bootstrap context in the stack trace.
  defp validate_node_source!(opts) do
    has_nodes     = Keyword.has_key?(opts, :nodes)
    has_nodes_mfa = Keyword.has_key?(opts, :nodes_mfa)

    if has_nodes and has_nodes_mfa do
      raise ArgumentError, "only one of :nodes or :nodes_mfa may be specified, not both"
    end

    if has_nodes do
      nodes = Keyword.fetch!(opts, :nodes)

      unless is_list(nodes) and Enum.all?(nodes, &is_atom/1) do
        raise ArgumentError, ":nodes must be a list of node atoms"
      end
    end

    if has_nodes_mfa do
      mfa = Keyword.fetch!(opts, :nodes_mfa)

      unless match?({m, f, a} when is_atom(m) and is_atom(f) and is_list(a), mfa) do
        raise ArgumentError, ":nodes_mfa must be a {module, function, args} tuple"
      end
    end

    if refresh_ms = Keyword.get(opts, :refresh_ms) do
      unless is_integer(refresh_ms) and refresh_ms > 0 do
        raise ArgumentError, ":refresh_ms must be a positive integer"
      end
    end
  end

  # ── Private — cluster config verification ────────────────────────────────────

  defp verify_cluster_config!(live_nodes) do
    local_cfg = export_config()

    Logger.info(
      "super_cache, cluster.bootstrap, verifying config against #{length(live_nodes)} peer(s)"
    )

    mismatches =
      live_nodes
      |> Task.async_stream(
        fn peer ->
          case fetch_remote_config(peer) do
            {:ok, remote_cfg} ->
              diffs =
                Enum.flat_map(@config_keys, fn key ->
                  local_val  = Map.get(local_cfg, key)
                  remote_val = Map.get(remote_cfg, key)

                  if local_val == remote_val do
                    []
                  else
                    [{key, local_val, remote_val}]
                  end
                end)

              {peer, diffs}

            {:error, reason} ->
              Logger.warning(
                "super_cache, cluster.bootstrap, could not fetch config from " <>
                  "#{inspect(peer)}: #{inspect(reason)} — skipping peer"
              )
              {peer, :unreachable}
          end
        end,
        timeout: 8_000,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, {_peer, :unreachable}} -> []
        {:ok, {_peer, []}}          -> []
        {:ok, {peer, diffs}}        -> [{peer, diffs}]
        _                           -> []
      end)

    if mismatches == [] do
      Logger.info("super_cache, cluster.bootstrap, config verified — all peers agree")
    else
      details =
        Enum.map_join(mismatches, "\n", fn {peer, diffs} ->
          diff_lines =
            Enum.map_join(diffs, "\n", fn {key, local_val, remote_val} ->
              "      #{inspect(key)}: local=#{inspect(local_val)}, peer=#{inspect(remote_val)}"
            end)

          "  peer #{inspect(peer)}:\n#{diff_lines}"
        end)

      raise ArgumentError, """
      SuperCache config mismatch — this node cannot join the cluster.

      Every node must be started with identical structural configuration.
      Mismatched keys:

      #{details}

      Fix: ensure start!/1 is called with the same opts on every node.
      """
    end
  end

  defp fetch_remote_config(peer) do
    try do
      cfg = :erpc.call(peer, __MODULE__, :export_config, [], 5_000)
      {:ok, cfg}
    catch
      kind, reason ->
        {:error, {kind, reason}}
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
