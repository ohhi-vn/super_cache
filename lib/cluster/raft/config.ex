defmodule SuperCache.Cluster.Raft.Config do
  @moduledoc """
  Configuration defaults and validation for the opt-in `:raft` replication mode.

  All settings are optional and live under `config :super_cache, :raft`:

      config :super_cache, :raft,
        data_dir: "/var/lib/my_app/super_cache_raft",
        command_timeout_ms: 2_000,
        wal_sync_method: :datasync,
        wal_max_size_bytes: 256_000_000,
        wal_max_batch_size: 8_192,
        election_timeout_min: 500,
        election_timeout_max: 2_000,
        heartbeat_interval: 100,
        segment_max_entries: 4096,
        segment_max_size_bytes: 64_000_000,
        segment_storage: false,
        segment_compute_checksums: true,
        snapshot_chunk_size: 65_536,
        max_pending_commands: 1_024,
        command_batch_linger_ms: 0,
        max_pipeline_count: 4_096,
        max_append_entries_rpc_batch_size: 128,
        compaction_entry_threshold: 8_192,
        compaction_wal_bytes: 134_217_728,
        compaction_interval_ms: 120_000

  The default `data_dir` is a per-node directory in the system temp dir;
  deployments that need durable Raft logs across restarts should set it
  explicitly. Throughput knobs mirror `RaftEx.App.System.default_config/0`;
  direct `:raft_ex` application env remains an escape hatch and is not the
  supported tuning path.
  """

  @defaults [
    data_dir: nil,
    command_timeout_ms: 2_000,
    wal_sync_method: :datasync,
    wal_max_size_bytes: 256_000_000,
    wal_max_batch_size: 8_192,
    election_timeout_min: 500,
    election_timeout_max: 2_000,
    heartbeat_interval: 100,
    segment_max_entries: 4_096,
    segment_max_size_bytes: 64_000_000,
    segment_storage: false,
    segment_compute_checksums: true,
    snapshot_chunk_size: 65_536,
    max_pending_commands: 1_024,
    command_batch_linger_ms: 0,
    max_pipeline_count: 4_096,
    max_append_entries_rpc_batch_size: 128,
    compaction_entry_threshold: 8_192,
    compaction_wal_bytes: 134_217_728,
    compaction_interval_ms: 120_000
  ]

  @sync_methods [:fsync, :datasync, :none]
  @data_dir_key {__MODULE__, :data_dir}

  @doc "Return the default configuration keyword list."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @doc "Return all Raft settings with application config overrides applied."
  @spec all() :: keyword
  def all do
    @defaults
    |> Keyword.merge(Application.get_env(:super_cache, :raft, []))
    |> Keyword.put(:data_dir, resolve_data_dir())
  end

  @doc "Return a single Raft setting."
  @spec get(atom) :: term
  def get(key), do: Keyword.fetch!(all(), key)

  @doc """
  Validate the configured settings, raising `ArgumentError` on invalid values.

  Bootstrap calls this before starting any Raft group so a bad configuration
  fails on startup rather than on the first write.
  """
  @spec validate!() :: :ok
  def validate! do
    config = all()

    unless is_binary(Keyword.fetch!(config, :data_dir)) do
      raise ArgumentError, ":raft data_dir must be a binary path"
    end

    timeout = Keyword.fetch!(config, :command_timeout_ms)

    unless is_integer(timeout) and timeout > 0 do
      raise ArgumentError, ":raft command_timeout_ms must be a positive integer"
    end

    unless Keyword.fetch!(config, :wal_sync_method) in @sync_methods do
      raise ArgumentError,
            ":raft wal_sync_method must be one of #{inspect(@sync_methods)}"
    end

    max_size = Keyword.fetch!(config, :wal_max_size_bytes)

    unless is_integer(max_size) and max_size > 0 do
      raise ArgumentError, ":raft wal_max_size_bytes must be a positive integer"
    end

    validate_positive!(config, :election_timeout_min)
    validate_positive!(config, :election_timeout_max)
    validate_positive!(config, :heartbeat_interval)
    validate_positive!(config, :segment_max_entries)
    validate_positive!(config, :segment_max_size_bytes)
    validate_positive!(config, :wal_max_batch_size)
    validate_positive!(config, :snapshot_chunk_size)
    validate_positive!(config, :max_pending_commands)
    validate_positive!(config, :max_pipeline_count)
    validate_positive!(config, :max_append_entries_rpc_batch_size)
    validate_positive!(config, :compaction_entry_threshold)
    validate_positive!(config, :compaction_wal_bytes)
    validate_positive!(config, :compaction_interval_ms)
    validate_non_negative!(config, :command_batch_linger_ms)
    validate_boolean!(config, :segment_storage)
    validate_boolean!(config, :segment_compute_checksums)

    if Keyword.fetch!(config, :election_timeout_min) >
         Keyword.fetch!(config, :election_timeout_max) do
      raise ArgumentError, ":raft election_timeout_min must not exceed election_timeout_max"
    end

    :ok
  end

  defp validate_positive!(config, key) do
    value = Keyword.fetch!(config, key)

    unless is_integer(value) and value > 0 do
      raise ArgumentError, ":raft #{key} must be a positive integer"
    end
  end

  defp validate_non_negative!(config, key) do
    value = Keyword.fetch!(config, key)

    unless is_integer(value) and value >= 0 do
      raise ArgumentError, ":raft #{key} must be a non-negative integer"
    end
  end

  defp validate_boolean!(config, key) do
    value = Keyword.fetch!(config, key)

    unless is_boolean(value) do
      raise ArgumentError, ":raft #{key} must be a boolean"
    end
  end

  @doc "Forget the memoized default data directory (used by tests)."
  @spec reset() :: :ok
  def reset do
    :persistent_term.erase(@data_dir_key)
    :ok
  end

  defp resolve_data_dir do
    case :persistent_term.get(@data_dir_key, nil) do
      nil ->
        dir =
          case Application.get_env(:super_cache, :raft, [])[:data_dir] do
            nil -> default_data_dir()
            configured -> configured
          end

        :persistent_term.put(@data_dir_key, dir)
        dir

      dir ->
        dir
    end
  end

  defp default_data_dir do
    suffix =
      node()
      |> Atom.to_string()
      |> String.replace(~r/[^a-zA-Z0-9]/, "_")

    Path.join(System.tmp_dir!(), "super_cache_raft_#{suffix}")
  end
end
