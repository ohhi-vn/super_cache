defmodule SuperCache.Cluster.WAL.Store do
  @moduledoc """
  Owns the Write-Ahead Log ETS tables.

  Separating table ownership from `SuperCache.Cluster.WAL` means the WAL
  GenServer can crash and restart without losing the pending entries or
  acknowledgement rows held in ETS.  The tables survive as long as this
  process — and the supervision tree — is alive.  They are *not* durable
  across a full VM restart, which is an explicit non-goal of the WAL.

  ## Tables

  - `SuperCache.Cluster.WAL` — WAL entries keyed by sequence number, plus
    the `{:seq, :counter}` bookkeeping row.
  - `SuperCache.Cluster.WAL_acks` — acknowledgement tracking rows.
  - `SuperCache.Cluster.WAL_watermarks` — per-partition applied-sequence
    watermarks used to make recovery idempotent.

  ## API

  The table names are also available as plain atoms via `wal_table/0`,
  `ack_table/0`, and `watermark_table/0` so callers never need to hard-code
  them.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger

  @wal_table SuperCache.Cluster.WAL
  @ack_table SuperCache.Cluster.WAL_acks
  @watermark_table SuperCache.Cluster.WAL_watermarks
  @ryw_table SuperCache.Cluster.Router.RywTracker
  @seq_key {:seq, :counter}

  # ── Public API ───────────────────────────────────────────────────────────────

  @doc "Starts the WAL store GenServer."
  @spec start_link(keyword) :: {:ok, pid} | {:error, term}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the WAL ETS table name."
  @spec wal_table() :: atom
  def wal_table, do: @wal_table

  @doc "Returns the WAL acknowledgement ETS table name."
  @spec ack_table() :: atom
  def ack_table, do: @ack_table

  @doc "Returns the per-partition watermark ETS table name."
  @spec watermark_table() :: atom
  def watermark_table, do: @watermark_table

  @doc "Returns the read-your-writes tracking ETS table name."
  @spec ryw_table() :: atom
  def ryw_table, do: @ryw_table

  # ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    ensure_table(@wal_table, :ordered_set)
    ensure_table(@ack_table, :set)
    ensure_table(@watermark_table, :set)
    ensure_table(@ryw_table, :set)

    :ets.insert_new(@wal_table, {@seq_key, 0})

    Logger.info("super_cache, wal.store, ETS tables ready")

    {:ok, %{}}
  end

  # ── Private ──────────────────────────────────────────────────────────────────

  defp ensure_table(name, type) do
    case :ets.info(name) do
      :undefined ->
        :ets.new(name, [
          type,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ ->
        name
    end
  end
end
