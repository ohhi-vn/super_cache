defmodule SuperCache.Internal.Stream do
  @moduledoc """
  Internal stream processor for SuperCache buffer queues.

  This module bridges the internal message-passing queue (`SuperCache.Internal.Queue`)
  with the caching layer (`SuperCache`). It creates a `Stream` that continuously
  pulls items from a named queue and flushes each drained batch into the cache
  via `SuperCache.put_batch!/1`.

  ## Design

  The stream is built using `Stream.resource/3`:
  1. **Accumulator** - The queue name/atom passed to `create/1`.
  2. **Next function** - Calls `Queue.get/1` to fetch a batch of items.
  3. **Cleanup function** - No-op, as the queue lifecycle is managed externally.

  `Queue.get/1` returns items in FIFO order, so sequential updates to the same
  key within one batch preserve the last write. The batch is flushed with a
  single `put_batch!/1` call. If a batch contains a malformed tuple, the flush
  retries item by item so valid tuples still land and only the bad item is
  logged.

  ## Warning

  This is an **internal** module. Do not use it directly in application code.
  Use `SuperCache.Buffer` or `SuperCache.lazy_put/1` instead.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Internal.Queue

  @typedoc "Queue name or PID used as the stream accumulator."
  @type queue_ref :: atom() | pid()

  @doc """
  Creates a `Stream` that continuously pulls items from the given queue.

  The stream yields batches of items returned by `Queue.get/1`.
  When the queue process is no longer running, the stream halts cleanly
  (the buffer runner decides whether to restart it).

  ## Examples

      stream = SuperCache.Internal.Stream.create(:my_buffer)
      Stream.take(stream, 10) |> Enum.to_list()
  """
  @spec create(queue_ref()) :: Enumerable.t()
  def create(queue_ref) when is_atom(queue_ref) or is_pid(queue_ref) do
    Stream.resource(
      fn -> queue_ref end,
      fn q ->
        SuperCache.Log.debug(fn -> "get data from queue" end)

        case Queue.get(q) do
          [] -> empty_batch(q)
          items -> {[items], q}
        end
      end,
      fn _queue ->
        :ok
      end
    )
  end

  defp empty_batch(queue_ref) do
    if Queue.down?(queue_ref), do: {:halt, queue_ref}, else: {[], queue_ref}
  end

  @doc """
  Consumes an enumerable and pushes each batch into the cache.

  Runs the stream to completion. Drained batches are written with
  `SuperCache.put_batch!/1`; if a batch contains a malformed tuple, the
  batch is retried item by item so valid tuples still land and only the bad
  item is logged.
  """
  @spec make_stream_pipe(Enumerable.t()) :: :ok
  def make_stream_pipe(enumable) do
    enumable
    |> Stream.each(&flush_batch/1)
    |> Stream.run()
  end

  @doc """
  Pushes a single item into the SuperCache.

  Wraps `SuperCache.put/1` with error handling. If the put fails, the error
  is logged but not raised, ensuring the stream continues processing.

  ## Examples

      SuperCache.Internal.Stream.push({:user, 1, "Alice"})
      # => :ok
  """
  @spec push(tuple()) :: :ok
  def push(data) when is_tuple(data) do
    SuperCache.put(data)

    SuperCache.Log.debug(fn ->
      "super_cache, internal_stream, pushed #{inspect(data)} to cache"
    end)

    :ok
  rescue
    err ->
      Logger.error(
        "super_cache, internal_stream, failed to push #{inspect(data)}: #{inspect(err)}"
      )

      :error
  end

  # ── Private helpers ──────────────────────────────────────────────────────────

  defp flush_batch(items) when is_list(items) do
    SuperCache.put_batch!(items)
  rescue
    _err ->
      # A malformed tuple poisoned the batch — retry individually so valid
      # items still land and only the bad one is logged.
      Enum.each(items, &safe_push/1)
  end

  defp flush_batch(item), do: safe_push(item)

  defp safe_push(data) do
    push(data)
  rescue
    err ->
      Logger.error(
        "super_cache, internal_stream, failed to push #{inspect(data)}: #{inspect(err)}"
      )

      :error
  catch
    kind, reason ->
      Logger.error(
        "super_cache, internal_stream, failed to push #{inspect(data)}: " <>
          inspect({kind, reason})
      )

      :error
  end
end
