defmodule SuperCache.Buffer do
  @moduledoc """
  Manages per-scheduler write buffers used by `SuperCache.lazy_put/1`.

  SuperCache creates one buffer process per online scheduler so that producers
  can enqueue data without cross-scheduler contention. Each buffer runs a
  continuous stream that pulls items from an internal queue and writes them
  into the cache.

  ## Design

  - **Scheduler affinity** — `enqueue/1` uses `:erlang.system_info(:scheduler_id)`
    to route data to the buffer running on the same scheduler, minimising
    context switches and cache-line bouncing.
  - **Persistent-term lookup** — Buffer names are stored in a tuple in
    `:persistent_term` at startup, making hot-path lookups allocation-free.
  - **Stream processing** — Each buffer is backed by `SuperCache.Internal.Queue`
    and `SuperCache.Internal.Stream`, which continuously drain the queue and
    push items into the cache.

  ## Lifecycle

  1. `start/1` is called by `SuperCache.Bootstrap` during startup.
  2. `enqueue/1` is called by application code via `SuperCache.lazy_put/1`.
  3. `stop/0` is called during shutdown to gracefully halt all buffers.

  ## Examples

      # Start buffers (usually done automatically by Bootstrap)
      SuperCache.Buffer.start(System.schedulers_online())

      # Enqueue data (usually via SuperCache.lazy_put/1)
      SuperCache.Buffer.enqueue({:user, 1, "Alice"})

      # Stop all buffers
      SuperCache.Buffer.stop()
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Internal.Queue, as: LibQueue
  alias SuperCache.Internal.Stream, as: LibStream

  @typedoc "Buffer process name (registered atom)."
  @type buffer_name :: atom()

  @pt_key {__MODULE__, :buffer_names}

  ## ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Starts one buffer stream per scheduler and registers their names in
  `:persistent_term`.

  Called automatically by `SuperCache.Bootstrap.start!/1`.

  ## Examples

      SuperCache.Buffer.start(4)
      # => :ok
  """
  @spec start(pos_integer) :: :ok
  def start(num_schedulers) when is_integer(num_schedulers) and num_schedulers > 0 do
    Logger.info("super_cache, buffer, starting #{num_schedulers} buffer(s)...")

    names =
      for id <- 1..num_schedulers do
        name = buffer_atom(id)

        # Spawn buffer stream in a separate process.
        # The stream runs until the queue is stopped or the VM shuts down.
        spawn(fn -> start_stream(name) end)

        name
      end

    # Store as a tuple for O(1) indexed access.
    :persistent_term.put(@pt_key, List.to_tuple(names))

    Logger.info("super_cache, buffer, #{num_schedulers} buffer(s) started successfully")
    :ok
  rescue
    err ->
      Logger.error("super_cache, buffer, failed to start buffers: #{inspect(err)}")
      :ok
  end

  @doc """
  Stops all buffer processes and removes the `:persistent_term` entry.

  Called automatically by `SuperCache.Bootstrap.stop/0`.

  Sends a `:stop` signal to each buffer queue, which gracefully terminates
  the associated stream. Returns `:ok` immediately (fire-and-forget).

  ## Examples

      SuperCache.Buffer.stop()
      # => :ok
  """
  @spec stop() :: :ok
  def stop() do
    SuperCache.Log.debug(fn -> "super_cache, buffer, stopping all buffers..." end)

    case :persistent_term.get(@pt_key, nil) do
      nil ->
        SuperCache.Log.debug(fn -> "super_cache, buffer, no buffers to stop (not started?)" end)
        :ok

      names_tuple ->
        count = tuple_size(names_tuple)
        stopped = stop_buffers(names_tuple, 0, count)

        :persistent_term.erase(@pt_key)

        Logger.info("super_cache, buffer, stopped #{stopped}/#{count} buffer(s)")
        :ok
    end
  rescue
    err ->
      Logger.error("super_cache, buffer, error during stop: #{inspect(err)}")
      :ok
  end

  @doc """
  Enqueues `data` into the buffer for the current scheduler.

  Uses `:erlang.system_info(:scheduler_id)` to route data to the correct
  buffer without random overhead or atom allocation.

  If the buffer system has not been started, logs a warning and returns
  `{:error, :not_started}`.

  ## Examples

      SuperCache.Buffer.enqueue({:user, 1, "Alice"})
      # => :ok
  """
  @spec enqueue(tuple) :: :ok | {:error, :not_started}
  def enqueue(data) when is_tuple(data) do
    case :persistent_term.get(@pt_key, nil) do
      nil ->
        Logger.warning("super_cache, buffer, enqueue called but buffers not started")
        {:error, :not_started}

      names_tuple ->
        # scheduler_id is 1-based; wrap with rem for safety.
        idx = rem(:erlang.system_info(:scheduler_id) - 1, tuple_size(names_tuple))
        buffer_name = elem(names_tuple, idx)

        LibQueue.add(buffer_name, data)

        SuperCache.Log.debug(fn ->
          "super_cache, buffer, enqueued to #{inspect(buffer_name)} (idx: #{idx})"
        end)

        :ok
    end
  end

  ## ── Private helpers ──────────────────────────────────────────────────────────

  # Atoms are built at startup — never at runtime — so the atom table is safe.
  defp buffer_atom(id), do: String.to_atom("SuperCache.Buffer_#{id}")

  defp start_stream(name) do
    Logger.info("super_cache, buffer, starting stream #{inspect(name)}")

    try do
      name
      |> LibQueue.start()
      |> LibStream.create()
      |> LibStream.make_stream_pipe()

      Logger.info("super_cache, buffer, stream #{inspect(name)} finished normally")
    rescue
      err ->
        Logger.error("super_cache, buffer, stream #{inspect(name)} raised: #{inspect(err)}")
        exit({:error, err})
    catch
      kind, reason ->
        Logger.error(
          "super_cache, buffer, stream #{inspect(name)} crashed: #{inspect({kind, reason})}"
        )

        exit({kind, reason})
    end
  end

  defp stop_buffers(names_tuple, idx, total) when idx < total do
    name = elem(names_tuple, idx)

    case Process.whereis(name) do
      nil ->
        SuperCache.Log.debug(fn ->
          "super_cache, buffer, #{inspect(name)} not found, skipping"
        end)

        stop_buffers(names_tuple, idx + 1, total)

      pid ->
        LibQueue.stop(pid)
        stop_buffers(names_tuple, idx + 1, total)
    end
  end

  defp stop_buffers(_names_tuple, idx, _total), do: idx
end
