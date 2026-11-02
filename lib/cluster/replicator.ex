defmodule SuperCache.Cluster.Replicator do
  @moduledoc """
  Applies replicated writes on the local node and handles bulk partition
  transfers when a new node joins or a full sync is requested.

  ## Replication Modes

  - `:async` — Fire-and-forget via a lightweight worker pool (`Task.Supervisor`).
    Eliminates per-operation `spawn/1` overhead.
  - `:sync` — Adaptive quorum writes. Returns `:ok` once a strict majority of
    replicas acknowledge, avoiding waits for slow stragglers.
  - `:strong` — Write-Ahead Log (WAL) based consistency. Replaces heavy 3PC
    with fast local write + async replication + majority ack (~200µs vs ~1500µs).

  Every call to `replicate/3` increments the appropriate
  `replicate_<mode>` counter in `SuperCache.Cluster.Metrics` and records
  a latency sample so the results are visible in
  `SuperCache.Cluster.Stats.api/0`.

  ## Async replication semantics

  An `:async` write returns after the local apply and a supervised handoff to
  the replication worker pool. Workers retry transient replica failures with
  bounded backoff, controlled by `:async_max_retries` (default `3`) and
  `:async_retry_backoff_ms` (default `50`) under the `:super_cache,
  :replication` application environment. Failures are logged and counted in
  `replication/async_failures`; deliveries that still fail after the retry
  budget are counted in `replication/async_drops`. A full or unavailable pool
  also increments `async_drops` immediately and the write still succeeds
  locally. Neither failure path blocks the caller, and no task-result message
  is left in the caller's mailbox.
  """

  require Logger

  alias SuperCache.Cluster.{Manager, Metrics, Quorum, WAL}
  alias SuperCache.{Partition, Storage}

  @batch_size 500
  @replica_timeout 5_000
  @quorum_timeout @replica_timeout + 500
  @push_retries 3
  @push_retry_backoff_ms 50

  # Worker pool for async replication.  Started by SuperCache.Application
  # under the supervision tree — see the comment there for why it must not
  # be created lazily.  Keep this atom in sync with application.ex.
  @pool_name __MODULE__.Pool

  # ── Public API ───────────────────────────────────────────────────────────────

  @doc false
  # Exposed for tests that need to exercise multi-batch partition streaming.
  def __batch_size__, do: @batch_size

  @spec replicate(non_neg_integer, atom, any) :: :ok | {:error, term}
  def replicate(partition_idx, op_name, op_arg \\ nil) do
    mode = SuperCache.Config.get_config(:replication_mode, :async)
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    if replicas == [] do
      :ok
    else
      metric_key = :"replicate_#{mode}"
      t0 = System.monotonic_time(:microsecond)

      result =
        try do
          do_replicate(mode, partition_idx, op_name, op_arg, replicas)
        rescue
          err -> {:error, err}
        end

      elapsed = System.monotonic_time(:microsecond) - t0
      Metrics.increment({:api, metric_key}, :calls)
      Metrics.push_latency({:api_latency_us, metric_key}, elapsed)

      case result do
        {:error, _} ->
          Metrics.increment({:api, metric_key}, :errors)
          Metrics.increment(:replication, failure_field(mode))

        _ ->
          :ok
      end

      result
    end
  end

  @doc """
  Apply a replicated operation on this node.

  Called via `:erpc` from the primary or worker pool — **do NOT call directly**.
  """
  @spec apply_op(non_neg_integer, atom, any) :: :ok
  def apply_op(partition_idx, op_name, op_arg) do
    partition = Partition.get_partition_by_idx(partition_idx)

    if partition == nil do
      raise ArgumentError, "unknown partition index: #{inspect(partition_idx)}"
    end

    case {op_name, op_arg} do
      {:put, records} when is_list(records) ->
        Storage.put(records, partition)

      {:put, record} ->
        Storage.put(record, partition)

      {:delete, key} ->
        Storage.delete(key, partition)

      {:delete_match, pattern} ->
        Storage.delete_match(pattern, partition)

      {:delete_all, _} ->
        Storage.delete_all(partition)

      {other, _} ->
        raise ArgumentError, "unknown replication op: #{inspect(other)}"
    end

    :ok
  end

  @doc """
  Push all records for `partition_idx` to `target_node` in bounded batches.

  Records are streamed from ETS in `@batch_size` chunks and each chunk is
  retried with bounded backoff. Successful batches are kept even when a later
  batch fails, and the error reports the exact failed and total record counts.
  """
  @spec push_partition(non_neg_integer, node) ::
          :ok | {:error, {:push_failed, non_neg_integer, non_neg_integer}}
  def push_partition(partition_idx, target_node) do
    partition = Partition.get_partition_by_idx(partition_idx)

    Logger.info(
      "super_cache, replicator, syncing partition #{partition_idx} → #{inspect(target_node)}"
    )

    {transferred, failed} = stream_partition(partition, partition_idx, target_node)
    total = transferred + failed

    Logger.info(
      "super_cache, replicator, pushed #{transferred}/#{total} record(s) for partition " <>
        "#{partition_idx} → #{inspect(target_node)}"
    )

    if failed == 0 do
      :ok
    else
      {:error, {:push_failed, failed, total}}
    end
  end

  defp stream_partition(partition, partition_idx, target_node) do
    {batch, transferred, failed} =
      Storage.scan(
        fn record, {batch, tr, fa} ->
          batch = [record | batch]

          if length(batch) >= @batch_size do
            {tr, fa} = flush_batch(partition_idx, target_node, Enum.reverse(batch), tr, fa)
            {[], tr, fa}
          else
            {batch, tr, fa}
          end
        end,
        {[], 0, 0},
        partition
      )

    flush_batch(partition_idx, target_node, Enum.reverse(batch), transferred, failed)
  end

  defp flush_batch(_idx, _target, [], transferred, failed), do: {transferred, failed}

  defp flush_batch(idx, target, batch, transferred, failed) do
    case send_batch(idx, target, batch, @push_retries) do
      :ok -> {transferred + length(batch), failed}
      :error -> {transferred, failed + length(batch)}
    end
  end

  defp send_batch(_idx, _target, _batch, 0), do: :error

  defp send_batch(idx, target, batch, retries) do
    :erpc.call(target, __MODULE__, :apply_op, [idx, :put, batch], 10_000)
    :ok
  rescue
    err ->
      Logger.warning(
        "super_cache, replicator, batch transfer to #{inspect(target)} failed " <>
          "(#{retries} retries left): #{inspect(err)}"
      )

      Process.sleep(@push_retry_backoff_ms)
      send_batch(idx, target, batch, retries - 1)
  catch
    kind, reason ->
      Logger.warning(
        "super_cache, replicator, batch transfer to #{inspect(target)} failed " <>
          "(#{retries} retries left): #{inspect({kind, reason})}"
      )

      Process.sleep(@push_retry_backoff_ms)
      send_batch(idx, target, batch, retries - 1)
  end

  @doc """
  Replicate a batch of operations to all replicas in a single `:erpc` call.

  Dramatically reduces network overhead for bulk writes by sending all
  operations in one message instead of one message per operation.
  """
  @spec replicate_batch(non_neg_integer, atom, [any]) :: :ok | {:error, term}
  def replicate_batch(partition_idx, op_name, op_args) when is_list(op_args) do
    {_primary, replicas} = Manager.get_replicas(partition_idx)

    if replicas == [] do
      :ok
    else
      t0 = System.monotonic_time(:microsecond)

      result =
        try do
          # OTP version differences: classic releases return {replies,
          # bad_nodes}; newer OTP returns a flat per-node result list where
          # failures appear as error entries.
          failures =
            multicall_failures(
              :erpc.multicall(
                replicas,
                __MODULE__,
                :apply_op_batch,
                [partition_idx, op_name, op_args],
                10_000
              ),
              length(replicas)
            )

          if failures > 0 do
            Logger.warning(
              "super_cache, replicator, batch replication incomplete for partition " <>
                "#{partition_idx}: #{failures}/#{length(replicas)} replica(s) failed"
            )

            {:error, {:replication_incomplete, failures}}
          else
            :ok
          end
        rescue
          err -> {:error, err}
        end

      elapsed = System.monotonic_time(:microsecond) - t0
      mode = SuperCache.Config.get_config(:replication_mode, :async)
      metric_key = :"replicate_batch_#{mode}"
      Metrics.increment({:api, metric_key}, :calls)
      Metrics.push_latency({:api_latency_us, metric_key}, elapsed)

      case result do
        {:error, _} ->
          Metrics.increment({:api, metric_key}, :errors)
          Metrics.increment(:replication, failure_field(mode))

        _ ->
          :ok
      end

      result
    end
  end

  @doc """
  Apply a batch of replicated operations on this node.

  Called via `:erpc` from the primary — **do NOT call directly**.
  """
  @spec apply_op_batch(non_neg_integer, atom, [any]) :: :ok
  def apply_op_batch(partition_idx, op_name, op_args) do
    partition = Partition.get_partition_by_idx(partition_idx)

    if partition == nil do
      raise ArgumentError, "unknown partition index: #{inspect(partition_idx)}"
    end

    case op_name do
      :put ->
        records =
          Enum.map(op_args, fn
            {key, record} -> {key, record}
            record when is_tuple(record) -> record
          end)

        Storage.put(records, partition)

      _ ->
        Enum.each(op_args, &apply_batch_op(op_name, &1, partition))
    end

    :ok
  end

  defp apply_batch_op(:delete, key, partition), do: Storage.delete(key, partition)

  defp apply_batch_op(:delete_match, pattern, partition),
    do: Storage.delete_match(pattern, partition)

  defp apply_batch_op(:delete_all, _arg, partition), do: Storage.delete_all(partition)

  defp apply_batch_op(other, _arg, _partition) do
    raise ArgumentError, "unknown replication op: #{inspect(other)}"
  end

  # A successful multicall entry is the bare return value (:ok here). Any
  # {:error, ...} tuple or caught-exception marker counts as a failure.
  defp async_replicate_worker(partition_idx, op_name, op_arg, replicas) do
    max_retries =
      Application.get_env(:super_cache, :replication, [])[:async_max_retries] || 3

    backoff_ms =
      Application.get_env(:super_cache, :replication, [])[:async_retry_backoff_ms] || 50

    do_async_replicate(partition_idx, op_name, op_arg, replicas, max_retries, backoff_ms, 0)
  end

  defp do_async_replicate(
         partition_idx,
         op_name,
         op_arg,
         replicas,
         max_retries,
         backoff_ms,
         attempt
       ) do
    failed =
      multicall_failures(
        :erpc.multicall(
          replicas,
          __MODULE__,
          :apply_op,
          [partition_idx, op_name, op_arg],
          5_000
        ),
        length(replicas)
      )

    if failed > 0 and attempt < max_retries do
      Process.sleep(backoff_ms * (attempt + 1))

      do_async_replicate(
        partition_idx,
        op_name,
        op_arg,
        replicas,
        max_retries,
        backoff_ms,
        attempt + 1
      )
    else
      if failed > 0 do
        Metrics.increment(:replication, :async_drops, failed)
      end

      :ok
    end
  rescue
    error ->
      Logger.warning(
        "super_cache, replicator, async replication failed (attempt #{attempt}): " <>
          inspect(error)
      )

      Metrics.increment(:replication, :async_failures)

      if attempt < max_retries do
        Process.sleep(backoff_ms * (attempt + 1))

        do_async_replicate(
          partition_idx,
          op_name,
          op_arg,
          replicas,
          max_retries,
          backoff_ms,
          attempt + 1
        )
      else
        :ok
      end
  catch
    kind, reason ->
      Logger.warning(
        "super_cache, replicator, async replication exited (attempt #{attempt}): " <>
          inspect({kind, reason})
      )

      Metrics.increment(:replication, :async_failures)

      if attempt < max_retries do
        Process.sleep(backoff_ms * (attempt + 1))

        do_async_replicate(
          partition_idx,
          op_name,
          op_arg,
          replicas,
          max_retries,
          backoff_ms,
          attempt + 1
        )
      else
        :ok
      end
  end

  # OTP 29 returns a flat list of per-node results; older releases return
  # {replies, bad_nodes}. Keep both shapes working while the project supports
  # Elixir ~> 1.15 across OTP versions. Dialyzer narrows the return type on the
  # running OTP, so the compatibility clause is annotated.
  @dialyzer {:nowarn_function, normalize_multicall: 1}
  defp normalize_multicall({replies, bad_nodes})
       when is_list(replies) and is_list(bad_nodes),
       do: {replies, bad_nodes}

  defp normalize_multicall(results) when is_list(results), do: {results, []}

  @dialyzer {:nowarn_function, multicall_failures: 2}
  defp multicall_failures(results, replica_count) do
    {replies, bad_nodes} = normalize_multicall(results)

    if bad_nodes == [] do
      replica_count - Enum.count(replies, &successful_reply?/1)
    else
      length(bad_nodes)
    end
  end

  defp successful_reply?({:error, _}), do: false
  defp successful_reply?({:error, _, _}), do: false
  defp successful_reply?(:caught), do: false
  defp successful_reply?(_), do: true

  defp failure_field(:async), do: :async_failures
  defp failure_field(:sync), do: :sync_failures
  defp failure_field(:strong), do: :strong_failures
  defp failure_field(_mode), do: :async_failures

  # ── Private ──────────────────────────────────────────────────────────────────

  defp do_replicate(:async, partition_idx, op_name, op_arg, replicas) do
    # Supervised fire-and-forget worker.  No result message is sent to the
    # caller; failures are logged and counted inside the worker.
    case Task.Supervisor.start_child(@pool_name, fn ->
           async_replicate_worker(partition_idx, op_name, op_arg, replicas)
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "super_cache, replicator, async replication pool full or unavailable: " <>
            inspect(reason)
        )

        Metrics.increment(:replication, :async_drops)
        :ok
    end
  end

  defp do_replicate(:sync, partition_idx, op_name, op_arg, replicas) do
    # Primary-inclusive quorum: the local apply already counts as one vote,
    # so require a strict majority of all copies (primary + replicas).
    # Duplicate acknowledgements are impossible because each replica is a
    # separate entry and Quorum rejects duplicate nodes.
    required = div(length(replicas) + 1, 2)

    entries =
      Enum.map(replicas, fn replica ->
        {replica,
         fn ->
           :erpc.call(
             replica,
             __MODULE__,
             :apply_op,
             [partition_idx, op_name, op_arg],
             @replica_timeout
           )
         end}
      end)

    case Quorum.await(entries, required, @quorum_timeout, pool: @pool_name) do
      {:ok, _value, _voters} -> :ok
      {:error, :timeout, _results} -> {:error, :quorum_timeout}
      {:error, reason, _results} -> {:error, reason}
    end
  end

  defp do_replicate(:strong, partition_idx, op_name, op_arg, _replicas) do
    # WAL-based strong consistency: replaces heavy 3PC.
    # ~200µs latency vs ~1500µs for 3PC.
    WAL.commit(partition_idx, [{op_name, op_arg}])
  end
end
