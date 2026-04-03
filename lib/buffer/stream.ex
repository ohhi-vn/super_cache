defmodule SuperCache.Internal.Stream do
  @moduledoc """
  Internal stream processor for SuperCache buffer queues.

  This module bridges the internal message-passing queue (`SuperCache.Internal.Queue`)
  with the caching layer (`SuperCache`). It creates a `Stream` that continuously
  pulls items from a named queue and pushes them into the cache via `SuperCache.put/1`.

  ## Design

  The stream is built using `Stream.resource/3`:
  1. **Accumulator** - The queue name/atom passed to `create/1`.
  2. **Next function** - Calls `Queue.get/1` to fetch a batch of items.
  3. **Cleanup function** - No-op, as the queue lifecycle is managed externally.

  The stream is consumed by `make_stream_pipe/1`, which pipes each item
  through `push/1` and runs the stream to completion.

  ## Error Handling

  - If `Queue.get/1` returns `{:error, :timeout}`, the stream terminates gracefully.
  - If `SuperCache.put/1` raises, the error is logged and the stream continues
    processing subsequent items (fault-tolerant design).

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
  If the queue returns `{:error, :timeout}`, the stream halts.

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
        {Queue.get(q), q}
      end,
      fn _queue ->
        :ok
      end
    )
  end

  @doc """
  Consumes an enumerable and pushes each item into the cache.

  Runs the stream to completion. Errors during `push/1` are caught and logged
  so that a single bad item does not halt the entire stream.

  ## Examples

      :my_buffer
      |> SuperCache.Internal.Stream.create()
      |> SuperCache.Internal.Stream.make_stream_pipe()
  """
  @spec make_stream_pipe(Enumerable.t()) :: :ok
  def make_stream_pipe(enumable) do
    enumable
    |> Stream.each(&safe_push/1)
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
    try do
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
  end

  # ── Private helpers ──────────────────────────────────────────────────────────

  defp safe_push(data) do
    push(data)
  end
end
