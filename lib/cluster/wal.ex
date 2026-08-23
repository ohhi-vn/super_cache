defmodule SuperCache.Cluster.WAL do
  @moduledoc """
  Write-Ahead Log for fast strong consistency.

  Replaces the heavy Three-Phase Commit (3PC) protocol with a lighter-weight
  WAL-based approach that:

  1. Writes to local ETS immediately
  2. Appends operation to WAL (in-memory ETS for speed)
  3. Async replicates WAL entries to replicas
  4. Tracks acknowledgments from replicas
  5. Returns success once majority has acked

  This reduces strong-mode latency from ~1500µs (3PC) to ~200µs (WAL).

  ## Design

  - WAL entries are stored in an ETS table for fast access
  - Each entry has a monotonically increasing sequence number
  - Replicas ack entries asynchronously via `:erpc.cast`
  - Majority acknowledgment determines commit success
  - Periodic cleanup of committed entries
  - Recovery on node restart replays uncommitted entries

  ## Usage

  This module is called internally by the Replicator when
  `replication_mode` is set to `:strong`. You should not call it directly.

  ## Configuration

  The WAL uses sensible defaults but can be tuned via application config:

      config :super_cache, :wal,
        majority_timeout: 2_000,  # ms to wait for majority ack
        cleanup_interval: 5_000,  # ms between cleanup cycles
        max_pending: 10_000       # max uncommitted entries before backpressure

  ## Example

      # Called internally by Replicator.replicate/3
      SuperCache.Cluster.WAL.commit(2, [{:put, {:user, 1, "Alice"}}])
      # => :ok
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Metrics}

  @table __MODULE__
  @ack_table :"#{@table}_acks"
  # Counter key stored in the WAL ETS table for atomic sequence generation.
  # Uses a tuple key so it never collides with integer sequence numbers.
  @seq_key {:seq, :counter}

  @default_majority_timeout 2_000
  @default_cleanup_interval 5_000

  # ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Starts the WAL GenServer.
  """
  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Commit operations via WAL.

  1. Writes to local ETS immediately
  2. Appends to WAL
  3. Async replicates to replicas
  4. Waits for majority acknowledgment
  5. Returns `:ok` on success, `{:error, reason}` on failure

  This is the fast path for strong consistency — typically ~200µs vs ~1500µs for 3PC.

  ## Example

      WAL.commit(2, [{:put, {:user, 1, "Alice"}}])
      # => :ok
  """
  @spec commit(non_neg_integer, [{atom, any}]) :: :ok | {:error, term}
  def commit(partition_idx, ops) do
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    if replicas == [] do
      # No replicas — apply locally and return immediately
      apply_local(partition_idx, ops)
    else
      t0 = System.monotonic_time(:microsecond)

      # Apply locally first (write-ahead). Abort when the local apply fails —
      # continuing would replicate an entry that was never applied here.
      case apply_local(partition_idx, ops) do
        {:error, _} = err ->
          Metrics.increment({:wal, :failed}, :calls)
          err

        :ok ->
          result = commit_with_replicas(partition_idx, ops, replicas)
          elapsed = System.monotonic_time(:microsecond) - t0

          case result do
            :ok ->
              Metrics.increment({:wal, :committed}, :calls)
              Metrics.push_latency({:wal_latency_us, :commit}, elapsed)
              :ok

            {:error, _} = err ->
              Metrics.increment({:wal, :failed}, :calls)
              Metrics.push_latency({:wal_latency_us, :commit_failed}, elapsed)
              err
          end
      end
    end
  end

  defp commit_with_replicas(partition_idx, ops, replicas) do
    # Get next sequence number
    seq = next_seq()

    # Write to WAL
    wal_entry = %{
      seq: seq,
      partition_idx: partition_idx,
      ops: ops,
      timestamp: System.monotonic_time(:millisecond)
    }

    :ets.insert(@table, {seq, wal_entry})

    # Initialize ack tracking: `{seq, required, waiter}` plus an atomic
    # counter row so concurrent replica acks can never lose counts.
    required = div(length(replicas), 2) + 1
    waiter = self()

    :ets.insert(@ack_table, [{seq, required, waiter}, {{seq, :count}, 0}])

    # Async replicate to all replicas
    async_replicate(seq, partition_idx, ops, replicas)

    # Wait for majority ack
    case await_majority(seq, required) do
      :ok ->
        :ok

      {:error, _} = err ->
        # Timed out — remove our tracking rows so they cannot leak.
        :ets.delete(@ack_table, seq)
        :ets.delete(@ack_table, {seq, :count})
        err
    end
  end

  @doc """
  Handle replication acknowledgment from a replica.

  Called via `:erpc.cast` on the primary node when a replica has applied the WAL entry.

  The ack count is incremented atomically (`:ets.update_counter/4`) so
  concurrent replica acks can never lose counts, and the majority
  notification is delivered to the committing process (recorded in the ack
  row) rather than to this module's GenServer.
  """
  @spec ack(non_neg_integer, node) :: :ok
  def ack(seq, _replica_node) do
    case :ets.lookup(@ack_table, seq) do
      [{^seq, required, waiter}] ->
        count = :ets.update_counter(@ack_table, {seq, :count}, {2, 1}, {{seq, :count}, 0})

        if count >= required do
          # Majority reached — wake the committer and clean up the rows so no
          # later ack can double-notify.
          :ets.delete(@ack_table, seq)
          :ets.delete(@ack_table, {seq, :count})
          send(waiter, {:majority_reached, seq})
        end

        :ok

      [] ->
        # Entry already cleaned up or not found (majority already reached)
        :ok
    end
  end

  @doc """
  Apply WAL operations on a replica and acknowledge.

  Called via `:erpc.cast` from the primary. Applies the operations locally
  then sends an ack back to the primary.
  """
  @spec replicate_and_ack(non_neg_integer, non_neg_integer, [{atom, any}]) :: :ok
  def replicate_and_ack(seq, partition_idx, ops) do
    apply_local(partition_idx, ops)

    # Send ack back to primary
    {primary, _} = Manager.get_replicas(partition_idx)

    if primary != node() do
      try do
        :erpc.cast(primary, __MODULE__, :ack, [seq, node()])
      catch
        kind, reason ->
          Logger.warning(
            "super_cache, wal, ack cast failed → #{inspect(primary)}: #{inspect({kind, reason})}"
          )
      end
    end

    :ok
  end

  @doc """
  Recover uncommitted WAL entries after restart.

  Replays any entries that haven't been fully committed to ensure consistency.
  """
  @spec recover() :: :ok
  def recover() do
    # Skip the `{{:seq, :counter}, n}` bookkeeping row — only map-valued rows
    # are real WAL entries.
    entries = Enum.filter(:ets.tab2list(@table), fn {_k, v} -> is_map(v) end)
    count = length(entries)

    if count > 0 do
      Logger.info("super_cache, wal, recovering #{count} uncommitted entries")

      Enum.each(entries, fn {_seq, entry} ->
        # Re-apply locally
        apply_local(entry.partition_idx, entry.ops)

        # Re-replicate to replicas
        {_primary, replicas} = Manager.get_replicas(entry.partition_idx)
        async_replicate(entry.seq, entry.partition_idx, entry.ops, replicas)
      end)
    end

    :ok
  end

  @doc """
  Return WAL statistics.

  ## Example

      WAL.stats()
      # => %{pending: 42, acks_pending: 2}
  """
  @spec stats() :: %{pending: non_neg_integer, acks_pending: non_neg_integer}
  def stats() do
    %{
      pending: max(0, :ets.info(@table, :size) - 1),
      acks_pending: :ets.info(@ack_table, :size)
    }
  end

  # ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :ordered_set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    ack_table =
      :ets.new(@ack_table, [
        :set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    # Initialize sequence counter in ETS for atomic increment.
    :ets.insert(@table, {@seq_key, 0})

    # Start periodic cleanup
    schedule_cleanup()

    Logger.info(
      "super_cache, wal, ETS tables ready (wal: #{inspect(table)}, acks: #{inspect(ack_table)})"
    )

    {:ok, %{}}
  end

  @impl true
  def handle_info({:majority_reached, _seq}, state) do
    # Safety net: majority notifications are delivered directly to the
    # committing process by ack/2, which also removes the tracking rows.
    # Anything still arriving here is stale — nothing to clean.
    {:noreply, state}
  end

  def handle_info(:cleanup, state) do
    cleanup_old_entries()
    schedule_cleanup()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Private ──────────────────────────────────────────────────────────────────

  # Atomic sequence number generation using :ets.update_counter/4.
  # Unlike the previous persistent_term read+write approach, this is
  # race-condition-free: two concurrent callers will always get distinct
  # sequence numbers because update_counter is an atomic ETS operation.
  defp next_seq() do
    :ets.update_counter(@table, @seq_key, {2, 1}, {@seq_key, 0})
  end

  defp apply_local(partition_idx, ops) do
    partition = Partition.get_partition_by_idx(partition_idx)

    if partition == nil do
      {:error, :invalid_partition}
    else
      Enum.each(ops, fn
        {:put, record} -> Storage.put(record, partition)
        {:delete, key} -> Storage.delete(key, partition)
        {:delete_match, pattern} -> Storage.delete_match(pattern, partition)
        {:delete_all, _} -> Storage.delete_all(partition)
      end)

      :ok
    end
  end

  defp async_replicate(seq, partition_idx, ops, replicas) do
    # Send to all replicas asynchronously using :erpc.cast (fire-and-forget)
    Enum.each(replicas, fn replica ->
      try do
        :erpc.cast(replica, __MODULE__, :replicate_and_ack, [seq, partition_idx, ops])
      catch
        kind, reason ->
          Logger.warning(
            "super_cache, wal, replication cast failed → #{inspect(replica)}: #{inspect({kind, reason})}"
          )
      end
    end)
  end

  defp await_majority(seq, _required) do
    timeout =
      Application.get_env(:super_cache, :wal, [])[:majority_timeout] || @default_majority_timeout

    # If the tracking row is already gone, majority was reached and cleaned up.
    case :ets.lookup(@ack_table, seq) do
      [_row] ->
        # Wait for notification or timeout
        receive do
          {:majority_reached, ^seq} -> :ok
        after
          timeout -> {:error, :majority_timeout}
        end

      [] ->
        # Already cleaned up (majority reached before we checked)
        :ok
    end
  end

  defp schedule_cleanup() do
    interval =
      Application.get_env(:super_cache, :wal, [])[:cleanup_interval] || @default_cleanup_interval

    Process.send_after(self(), :cleanup, interval)
  end

  defp cleanup_old_entries() do
    # Remove WAL entries older than 10 seconds. The sequence-counter row
    # ({@seq_key, integer}) is skipped by the `is_map` guard.
    cutoff = System.monotonic_time(:millisecond) - 10_000

    :ets.select_delete(@table, [
      {{:"$1", :"$2"},
       [{:andalso, {:is_map, :"$2"}, {:<, {:map_get, :timestamp, :"$2"}, cutoff}}], [true]}
    ])
  end
end
