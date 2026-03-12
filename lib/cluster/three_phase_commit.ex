defmodule SuperCache.Cluster.ThreePhaseCommit do
  @moduledoc """
  Three-phase commit (3PC) coordinator.

  Every `commit/2` call records a latency sample and increments either
  the `:committed` or `:aborted` counter (plus per-phase failure counters
  on abort) via `SuperCache.Cluster.Stats.record_tpc/2`.  These are
  visible in `SuperCache.Cluster.Stats.three_phase_commit/0`.

  ## Coordinator contract

  `commit/2` **must be called from the partition's primary node**.
  The Router enforces this for all writes that go through
  `SuperCache.Cluster.Router`.  If `commit/2` is called from a non-primary
  node (e.g. in a direct test), `apply_local/2` will write to the wrong
  node's ETS table and the primary will be left without data.

  ## ops fallback in handle_commit

  `phase_commit/4` passes the full `ops` list to every replica alongside
  the `txn_id`.  `handle_commit/3` uses the locally registered ops first
  (set by `handle_prepare/3`); if the TxnRegistry entry is absent — due to
  a prepare message being lost or a race during startup — it falls back to
  the coordinator-supplied ops so the write still completes rather than
  being silently skipped.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, TxnRegistry}
  alias SuperCache.Cluster.Stats

  @prepare_timeout 3_000
  @pre_commit_timeout 2_000
  @commit_timeout 3_000

  @type txn_id :: binary
  @type op ::
          {:put, tuple}
          | {:delete, any}
          | {:delete_match, tuple}
          | {:delete_all, nil}
  @type op_list :: [op]

  # ── Public API ───────────────────────────────────────────────────────────────

  @spec commit(non_neg_integer, op_list) :: :ok | {:error, term}
  def commit(partition_idx, ops) do
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    if replicas == [] do
      apply_local(partition_idx, ops)
      :ok
    else
      txn_id = generate_txn_id()
      TxnRegistry.register(txn_id, partition_idx, ops, replicas)

      SuperCache.Log.debug(fn ->
        "super_cache, 3pc, txn=#{txn_id} starting on #{length(replicas)} replica(s)"
      end)

      t0 = System.monotonic_time(:microsecond)
      result = run_phases(txn_id, partition_idx, ops, replicas)
      elapsed = System.monotonic_time(:microsecond) - t0

      TxnRegistry.remove(txn_id)

      case result do
        :ok ->
          Stats.record_tpc(:committed, latency_us: elapsed)

        {:error, {:vote_no, _}} ->
          Stats.record_tpc(:aborted, phase: :prepare)

        {:error, {:prepare_timeout, _}} ->
          Stats.record_tpc(:aborted, phase: :prepare)

        {:error, {:pre_commit_failed, _}} ->
          Stats.record_tpc(:aborted, phase: :pre_commit)

        {:error, {:commit_failed, _}} ->
          Stats.record_tpc(:aborted, phase: :commit)

        {:error, _} ->
          Stats.record_tpc(:aborted, [])
      end

      result
    end
  end

  # ── Participant callbacks ────────────────────────────────────────────────────

  @doc false
  @spec handle_prepare(txn_id, non_neg_integer, op_list) ::
          :vote_yes | {:vote_no, term}
  def handle_prepare(txn_id, partition_idx, ops) do
    case validate_ops(ops) do
      :ok ->
        TxnRegistry.register(txn_id, partition_idx, ops, [])
        SuperCache.Log.debug(fn -> "super_cache, 3pc, txn=#{txn_id} VOTE_YES" end)
        :vote_yes

      {:error, reason} ->
        Logger.warning("super_cache, 3pc, txn=#{txn_id} VOTE_NO: #{inspect(reason)}")
        {:vote_no, reason}
    end
  end

  @doc false
  @spec handle_pre_commit(txn_id) :: :ack_pre_commit
  def handle_pre_commit(txn_id) do
    TxnRegistry.mark_pre_committed(txn_id)
    SuperCache.Log.debug(fn -> "super_cache, 3pc, txn=#{txn_id} ACK_PRE_COMMIT" end)
    :ack_pre_commit
  end

  @doc false
  # ops_fallback is supplied by the coordinator in phase_commit/4 so that a
  # replica that missed PREPARE (no TxnRegistry entry) can still apply the
  # write rather than silently skipping it.
  @spec handle_commit(txn_id, non_neg_integer, op_list) :: :ack_commit
  def handle_commit(txn_id, partition_idx, ops_fallback \\ []) do
    ops =
      case TxnRegistry.get(txn_id) do
        nil ->
          Logger.warning(
            "super_cache, 3pc, txn=#{txn_id} COMMIT with no log entry — " <>
              "applying #{length(ops_fallback)} coordinator-supplied op(s)"
          )

          ops_fallback

        %{ops: registered_ops} ->
          TxnRegistry.remove(txn_id)
          registered_ops
      end

    apply_local(partition_idx, ops)
    SuperCache.Log.debug(fn -> "super_cache, 3pc, txn=#{txn_id} ACK_COMMIT" end)
    :ack_commit
  end

  @doc false
  @spec handle_abort(txn_id) :: :ack_abort
  def handle_abort(txn_id) do
    TxnRegistry.remove(txn_id)
    SuperCache.Log.debug(fn -> "super_cache, 3pc, txn=#{txn_id} ABORTED on #{node()}" end)
    :ack_abort
  end

  # ── Recovery ────────────────────────────────────────────────────────────────

  @spec recover() :: :ok
  def recover() do
    TxnRegistry.list_all()
    |> Enum.each(fn {txn_id, txn} ->
      case txn.state do
        :pre_committed ->
          Logger.info("super_cache, 3pc, recovery: committing in-doubt txn=#{txn_id}")
          apply_local(txn.partition_idx, txn.ops)
          TxnRegistry.remove(txn_id)
          Stats.record_tpc(:recovered_committed, [])

        :prepared ->
          Logger.info("super_cache, 3pc, recovery: aborting uncertain txn=#{txn_id}")
          TxnRegistry.remove(txn_id)
          Stats.record_tpc(:recovered_aborted, [])

        other ->
          Logger.warning(
            "super_cache, 3pc, recovery: unknown state #{inspect(other)} " <>
              "for txn=#{txn_id}"
          )

          TxnRegistry.remove(txn_id)
      end
    end)

    :ok
  end

  # ── Private — phase orchestration ───────────────────────────────────────────

  defp run_phases(txn_id, partition_idx, ops, replicas) do
    with :ok <- phase_prepare(txn_id, partition_idx, ops, replicas),
         :ok <- phase_pre_commit(txn_id, replicas),
         # Pass ops so replicas can fall back if their TxnRegistry entry is gone.
         :ok <- phase_commit(txn_id, partition_idx, ops, replicas) do
      apply_local(partition_idx, ops)
      Logger.info("super_cache, 3pc, txn=#{txn_id} committed on #{length(replicas) + 1} node(s)")
      :ok
    else
      {:error, reason} = err ->
        broadcast_abort(txn_id, replicas)
        Logger.warning("super_cache, 3pc, txn=#{txn_id} aborted: #{inspect(reason)}")
        err
    end
  end

  defp phase_prepare(txn_id, partition_idx, ops, replicas) do
    results =
      replicas
      |> Enum.map(fn n ->
        Task.async(fn ->
          try do
            {n,
             :erpc.call(
               n,
               __MODULE__,
               :handle_prepare,
               [txn_id, partition_idx, ops],
               @prepare_timeout
             )}
          catch
            kind, reason -> {n, {:error, {kind, reason}}}
          end
        end)
      end)
      |> Task.await_many(@prepare_timeout + 500)

    no_votes = Enum.filter(results, &match?({_, {:vote_no, _}}, &1))
    errors = Enum.filter(results, &match?({_, {:error, _}}, &1))

    cond do
      no_votes != [] ->
        [{n, {:vote_no, r}} | _] = no_votes
        {:error, {:vote_no, node: n, reason: r}}

      errors != [] ->
        [{n, {:error, r}} | _] = errors
        {:error, {:prepare_timeout, node: n, reason: r}}

      true ->
        :ok
    end
  end

  defp phase_pre_commit(txn_id, replicas) do
    results =
      replicas
      |> Enum.map(fn n ->
        Task.async(fn ->
          try do
            {n, :erpc.call(n, __MODULE__, :handle_pre_commit, [txn_id], @pre_commit_timeout)}
          catch
            kind, reason -> {n, {:error, {kind, reason}}}
          end
        end)
      end)
      |> Task.await_many(@pre_commit_timeout + 500)

    case Enum.filter(results, &match?({_, {:error, _}}, &1)) do
      [] -> :ok
      [{n, {:error, r}} | _] -> {:error, {:pre_commit_failed, node: n, reason: r}}
    end
  end

  # ops is now passed to handle_commit so replicas can apply the write even
  # when their TxnRegistry entry is absent (missed PREPARE race).
  defp phase_commit(txn_id, partition_idx, ops, replicas) do
    results =
      replicas
      |> Enum.map(fn n ->
        Task.async(fn ->
          try do
            {n,
             :erpc.call(
               n,
               __MODULE__,
               :handle_commit,
               [txn_id, partition_idx, ops],
               @commit_timeout
             )}
          catch
            kind, reason -> {n, {:error, {kind, reason}}}
          end
        end)
      end)
      |> Task.await_many(@commit_timeout + 500)

    case Enum.filter(results, &match?({_, {:error, _}}, &1)) do
      [] -> :ok
      [{n, {:error, r}} | _] -> {:error, {:commit_failed, node: n, reason: r}}
    end
  end

  defp broadcast_abort(txn_id, replicas) do
    Enum.each(replicas, fn n ->
      spawn(fn ->
        try do
          :erpc.call(n, __MODULE__, :handle_abort, [txn_id], 5_000)
        catch
          _, _ -> :ok
        end
      end)
    end)
  end

  defp apply_local(partition_idx, ops) do
    partition = Partition.get_partition_by_idx(partition_idx)

    Enum.each(ops, fn
      {:put, record} -> Storage.put(record, partition)
      {:delete, key} -> Storage.delete(key, partition)
      {:delete_match, pattern} -> Storage.delete_match(pattern, partition)
      {:delete_all, _} -> Storage.delete_all(partition)
    end)
  end

  defp generate_txn_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp validate_ops(ops) when is_list(ops) do
    invalid =
      Enum.find(ops, fn
        {:put, t} when is_tuple(t) -> false
        {:delete, _} -> false
        {:delete_match, t} when is_tuple(t) -> false
        {:delete_all, nil} -> false
        _ -> true
      end)

    case invalid do
      nil -> :ok
      bad -> {:error, {:invalid_op, bad}}
    end
  end

  defp validate_ops(_), do: {:error, :ops_must_be_list}
end
