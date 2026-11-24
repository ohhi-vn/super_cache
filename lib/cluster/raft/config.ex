defmodule SuperCache.Cluster.Raft.Config do
  @moduledoc """
  Configuration defaults and validation for the opt-in `:raft` replication mode.

  All settings are optional and live under `config :super_cache, :raft`:

      config :super_cache, :raft,
        data_dir: "/var/lib/my_app/super_cache_raft",
        command_timeout_ms: 2_000,
        wal_sync_method: :datasync,
        wal_max_size_bytes: 256_000_000

  The default `data_dir` is a per-node directory in the system temp dir;
  deployments that need durable Raft logs across restarts should set it
  explicitly.
  """

  @defaults [
    data_dir: nil,
    command_timeout_ms: 2_000,
    wal_sync_method: :datasync,
    wal_max_size_bytes: 256_000_000
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

    :ok
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
