defmodule SuperCache.Cluster.TxnRegistry do
  @moduledoc """
  In-memory transaction log for the three-phase commit protocol.

  Each in-flight transaction is stored as a map entry keyed by its
  `txn_id` binary.  The log is held in an ETS table owned by this
  GenServer so that:

  - Reads are lock-free (`:public` + `:read_concurrency`).
  - The table survives coordinator or participant crashes as long as the
    owning VM is alive (the ETS table is linked to this GenServer's
    lifetime, not to the caller's).
  - On node restart, `SuperCache.Cluster.ThreePhaseCommit.recover/0` can
    iterate the table to resolve in-doubt transactions.

  ## Transaction states

  | State           | Meaning                                                   |
  |-----------------|-----------------------------------------------------------|
  | `:prepared`     | PREPARE accepted; waiting for PRE_COMMIT or ABORT        |
  | `:pre_committed`| PRE_COMMIT acknowledged; commit is safe even after crash |
  | `:committed`    | COMMIT applied; entry removed immediately after          |
  | `:aborted`      | ABORT received; entry removed immediately after          |

  ## Memory management

  Committed and aborted entries are deleted synchronously by
  `ThreePhaseCommit` after each successful/failed transaction.
  Stale `:prepared` entries (e.g. from a coordinator crash before
  PRE_COMMIT) are cleaned up by `recover/0` on the next startup.
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger

  @table __MODULE__

  # ── Lifecycle ────────────────────────────────────────────────────────────────

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    @table = :ets.new(@table, [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])

    Logger.info("super_cache, txn_registry, ETS table #{inspect(@table)} ready")
    {:ok, %{}}
  end

  # ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Register a new transaction in state `:prepared`.

  `replicas` is the list of participant nodes (may be empty on participants
  themselves — they only need `ops` and `partition_idx`).
  """
  @spec register(binary, non_neg_integer, list, [node]) :: :ok
  def register(txn_id, partition_idx, ops, replicas) do
    entry = %{
      txn_id:        txn_id,
      partition_idx: partition_idx,
      ops:           ops,
      replicas:      replicas,
      state:         :prepared,
      inserted_at:   System.monotonic_time(:millisecond)
    }

    :ets.insert(@table, {txn_id, entry})
    :ok
  end

  @doc "Transition `txn_id` to `:pre_committed`."
  @spec mark_pre_committed(binary) :: :ok
  def mark_pre_committed(txn_id) do
    update_state(txn_id, :pre_committed)
  end

  @doc "Look up a transaction by id.  Returns the entry map or `nil`."
  @spec get(binary) :: map | nil
  def get(txn_id) do
    case :ets.lookup(@table, txn_id) do
      [{^txn_id, entry}] -> entry
      []                 -> nil
    end
  end

  @doc "Remove a transaction entry (called after commit or abort)."
  @spec remove(binary) :: :ok
  def remove(txn_id) do
    :ets.delete(@table, txn_id)
    :ok
  end

  @doc "Return all transaction entries as `[{txn_id, entry}]`."
  @spec list_all() :: [{binary, map}]
  def list_all() do
    :ets.tab2list(@table)
  end

  @doc "Return the number of in-flight transactions (useful for tests)."
  @spec count() :: non_neg_integer
  def count() do
    :ets.info(@table, :size)
  end

  # ── Private ──────────────────────────────────────────────────────────────────

  defp update_state(txn_id, new_state) do
    case :ets.lookup(@table, txn_id) do
      [{^txn_id, entry}] ->
        :ets.insert(@table, {txn_id, %{entry | state: new_state}})
        :ok

      [] ->
        Logger.warning("super_cache, txn_registry, update_state: txn=#{txn_id} not found")
        :ok
    end
  end
end
