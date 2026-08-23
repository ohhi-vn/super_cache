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

  # Sentinel stored in `:persistent_term` after a deliberate stop/0 — lets
  # stream runners distinguish "shutting down" from "queue crashed".
  @stopped :buffers_stopped

  # Restart policy for buffer streams (see run_stream/1).
  @backoff_base_ms 250
  @backoff_max_ms 5_000
  @max_restarts 10

  # A run that lasted at least this long is considered healthy and resets the
  # consecutive-restart budget — mirrors supervisor restart intensity.
  @healthy_run_ms 30_000

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
  Stops all buffer processes and marks the buffer system as stopped.

  Called automatically by `SuperCache.Bootstrap.stop/0`.

  Sends a `:stop` signal to each buffer queue, which gracefully terminates
  the associated stream, then waits briefly for each queue to terminate so a
  following `start/1` never races the old processes for their names.
  """
  @spec stop() :: :ok
  def stop() do
    SuperCache.Log.debug(fn -> "super_cache, buffer, stopping all buffers..." end)

    case :persistent_term.get(@pt_key, nil) do
      nil ->
        SuperCache.Log.debug(fn -> "super_cache, buffer, no buffers to stop (not started?)" end)
        :ok

      names_tuple when is_tuple(names_tuple) ->
        # Mark deliberate shutdown BEFORE killing queues so stream runners
        # exit quietly instead of treating the stop as a crash to recover.
        :persistent_term.put(@pt_key, @stopped)

        count = tuple_size(names_tuple)
        stopped = stop_buffers(names_tuple, 0, count, 0)

        Logger.info("super_cache, buffer, stopped #{stopped}/#{count} buffer(s)")
        :ok

      @stopped ->
        SuperCache.Log.debug(fn -> "super_cache, buffer, already stopped" end)
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
  @spec enqueue(tuple) :: :ok | {:error, :not_started} | {:error, :process_down}
  def enqueue(data) when is_tuple(data) do
    case :persistent_term.get(@pt_key, nil) do
      state when state in [nil, @stopped] ->
        Logger.warning("super_cache, buffer, enqueue called but buffers not started")
        {:error, :not_started}

      names_tuple ->
        # scheduler_id is 1-based; wrap with rem for safety.
        idx = rem(:erlang.system_info(:scheduler_id) - 1, tuple_size(names_tuple))
        buffer_name = elem(names_tuple, idx)

        case LibQueue.add(buffer_name, data) do
          :ok ->
            SuperCache.Log.debug(fn ->
              "super_cache, buffer, enqueued to #{inspect(buffer_name)} (idx: #{idx})"
            end)

            :ok

          {:error, :process_down} ->
            # The runner should restart the queue shortly; surface the drop
            # loudly instead of losing writes silently.
            Logger.warning(
              "super_cache, buffer, enqueue to #{inspect(buffer_name)} dropped: buffer restarting"
            )

            {:error, :process_down}
        end
    end
  end

  ## ── Private helpers ──────────────────────────────────────────────────────────

  # Atoms are built at startup — never at runtime — so the atom table is safe.
  defp buffer_atom(id), do: String.to_atom("SuperCache.Buffer_#{id}")

  # Buffers are raw spawned processes with no supervisor. This runner makes
  # them self-healing: without it a single crash would kill the buffer
  # permanently and every subsequent lazy_put/1 write would be silently
  # dropped for the rest of the cache's lifetime.
  defp start_stream(name) do
    SuperCache.Log.debug(fn -> "super_cache, buffer, starting stream #{inspect(name)}" end)
    run_stream(name, 0)
  end

  defp run_stream(name, restarts) do
    t0 = System.monotonic_time(:millisecond)

    outcome =
      try do
        ensure_queue(name)

        name
        |> LibStream.create()
        |> LibStream.make_stream_pipe()

        :ended
      rescue
        err -> {:failed, {:error, err}}
      catch
        kind, reason -> {:failed, {kind, reason}}
      end

    # A run that lasted a while counts as healthy and resets the restart
    # budget; only rapid consecutive failures exhaust it.
    restarts =
      if System.monotonic_time(:millisecond) - t0 >= @healthy_run_ms, do: 0, else: restarts + 1

    case outcome do
      {:failed, reason} ->
        retry_or_give_up(name, restarts, "stream crashed (#{inspect(reason)})")

      :ended ->
        if buffers_started?() do
          # Queue died unexpectedly while the buffer system is active.
          retry_or_give_up(name, restarts, "queue ended while buffers still running")
        else
          SuperCache.Log.debug(fn ->
            "super_cache, buffer, stream #{inspect(name)} finished normally"
          end)
        end
    end
  end

  defp retry_or_give_up(_name, restarts, _why) when restarts > @max_restarts do
    Logger.error(
      "super_cache, buffer, exceeded #{@max_restarts} consecutive restarts — " <>
        "buffer disabled; lazy_put/1 writes will be dropped until buffers are restarted"
    )
  end

  defp retry_or_give_up(name, restarts, why) do
    delay = min(@backoff_base_ms * restarts, @backoff_max_ms)

    Logger.warning(
      "super_cache, buffer, #{why} for #{inspect(name)} — restarting in #{delay}ms " <>
        "(restart #{restarts}/#{@max_restarts})"
    )

    Process.sleep(delay)

    # Shutdown may have begun while we waited — do not resurrect buffers then.
    if buffers_started?(), do: run_stream(name, restarts)
  end

  # Reuse a live queue so buffered lazy writes survive a stream crash;
  # only recreate when the queue process is really gone.
  #
  # Registration races between concurrent runners (e.g. an old generation
  # waking up just after a new Buffer.start) raise ArgumentError from
  # LibQueue.start — that is contention, not a crash, so it is absorbed
  # here instead of burning the stream's restart budget.
  defp ensure_queue(name), do: ensure_queue(name, 20)

  defp ensure_queue(_name, 0), do: raise("could not acquire queue")

  defp ensure_queue(name, attempts) do
    case Process.whereis(name) do
      nil ->
        try do
          LibQueue.start(name)
          :ok
        rescue
          ArgumentError ->
            Process.sleep(25)
            ensure_queue(name, attempts - 1)
        end

      _pid ->
        :ok
    end
  end

  # True while `start/1` is in effect. A deliberate stop/0 stores the
  # @stopped sentinel so runners exit quietly instead of recovering.
  defp buffers_started?() do
    case :persistent_term.get(@pt_key, nil) do
      names_tuple when is_tuple(names_tuple) -> true
      _ -> false
    end
  end

  defp stop_buffers(names_tuple, idx, total, stopped) when idx < total do
    name = elem(names_tuple, idx)

    case Process.whereis(name) do
      nil ->
        SuperCache.Log.debug(fn ->
          "super_cache, buffer, #{inspect(name)} not found, skipping"
        end)

        stop_buffers(names_tuple, idx + 1, total, stopped)

      pid ->
        # Wait briefly for the queue to die so a following Buffer.start cycle
        # never races the old process for its registered name.
        ref = Process.monitor(pid)
        LibQueue.stop(pid)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        after
          1_000 -> :ok
        end

        Process.demonitor(ref, [:flush])
        stop_buffers(names_tuple, idx + 1, total, stopped + 1)
    end
  end

  defp stop_buffers(_names_tuple, _idx, _total, stopped), do: stopped
end
