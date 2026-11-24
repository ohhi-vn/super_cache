defmodule SuperCache.Cluster.Raft.Store do
  @moduledoc """
  Owns the ETS tables used by the Raft adapter.

  Table ownership lives here rather than in a Raft state machine or server so
  that a process restart does not lose the applied-index watermarks. The table
  is not durable across a full VM restart; the Raft log and snapshots are the
  durable state.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger

  @applied_table :super_cache_raft_applied

  @doc "Starts the Raft store GenServer."
  @spec start_link(keyword) :: {:ok, pid} | {:error, term}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the applied-index ETS table name."
  @spec applied_table() :: atom
  def applied_table, do: @applied_table

  @impl true
  def init(_opts) do
    case :ets.info(@applied_table) do
      :undefined ->
        :ets.new(@applied_table, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ ->
        :ok
    end

    Logger.info("super_cache, raft.store, ETS tables ready")
    {:ok, %{}}
  end
end
