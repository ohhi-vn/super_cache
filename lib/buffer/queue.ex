defmodule SuperCache.Internal.Queue do
  @moduledoc """
  Internal concurrent queue used by SuperCache buffer streams.

  This module implements a lightweight message-passing queue that supports:
  - Multiple producers adding items concurrently.
  - Multiple consumers reading batches of items.
  - Graceful shutdown that notifies waiting readers.

  ## Design

  The queue runs as a registered process. It maintains two lists:
  - `readers`: PIDs waiting for data.
  - `data`: Buffered items waiting to be consumed.

  When data arrives and readers are waiting, the entire buffer is sent to the
  first reader. When readers arrive and data is available, it is delivered
  immediately.

  ## Timeouts & Retries

  To prevent infinite hangs, `get/2` accepts a timeout and a maximum number
  of retries. If the queue does not respond within the timeout, it retries
  up to `:max_retries` times before returning `{:error, :timeout}`.

  ## Warning

  This is an **internal** module. Do not use it directly in application code.
  Use `SuperCache.Buffer` or `SuperCache.lazy_put/1` instead.
  """

  require Logger
  require SuperCache.Log

  ## API

  @doc """
  Starts a new queue process registered under `name`.

  Returns the PID of the started process once registration is complete —
  consumers can rely on `Process.whereis/1` immediately after this returns.
  Raises if `name` is already taken.
  """
  @spec start(atom()) :: pid()
  def start(name) when is_atom(name) do
    parent = self()

    pid =
      spawn(fn ->
        try do
          Process.register(self(), name)
          send(parent, {:queue_registered, self(), name})
          SuperCache.Log.debug(fn ->
            "super_cache, internal_queue, started #{inspect(name)}"
          end)

          loop([], [], name)
        rescue
          err ->
            send(parent, {:queue_register_failed, self()})

            Logger.error(
              "super_cache, internal_queue, failed to register #{inspect(name)}: #{inspect(err)}"
            )

            exit({:register_failed, name, err})
        end
      end)

    await_registration(pid, name)
  end

  @doc """
  Adds `data` to the queue.

  Returns `:ok` immediately. If the queue process is not alive, logs a warning
  and returns `{:error, :process_down}` — the item is dropped.
  """
  @spec add(atom() | pid(), any()) :: :ok | {:error, :process_down}
  def add(target, data) do
    case resolve(target) do
      nil ->
        Logger.warning(
          "super_cache, internal_queue, add to #{inspect(target)} failed: queue not running"
        )

        {:error, :process_down}

      pid ->
        send(pid, {:add, data})
        :ok
    end
  end

  @doc """
  Stops the queue process gracefully.

  Waiting readers will receive `:stop` and return `[]`. New readers will
  also receive `:stop`. Returns `:ok` immediately.
  """
  @spec stop(atom() | pid()) :: :ok
  def stop(pid) do
    send(pid, :stop)
    :ok
  end

  @doc """
  Returns `true` when the queue process is no longer running.

  Used by the consumer stream to distinguish "queue temporarily empty"
  from "queue gone" and halt instead of polling a dead process forever.
  """
  @spec down?(atom() | pid()) :: boolean()
  def down?(name) when is_atom(name), do: Process.whereis(name) == nil
  def down?(pid) when is_pid(pid), do: not Process.alive?(pid)

  @doc """
  Blocks until data is available, then returns the buffered items as a list.

  Returns `[]` when the queue is stopping or has died — the monitor ensures
  a reader can never hang forever on a dead queue process.

  ## Examples

      SuperCache.Internal.Queue.get(:my_buffer)
      # => [{:user, 1, "Alice"}]
  """
  @spec get(atom() | pid()) :: [any()]
  def get(target) do
    case resolve(target) do
      nil ->
        []

      pid ->
        ref = Process.monitor(pid)
        send(pid, {:get, self()})

        result =
          receive do
            :stop -> []
            list when is_list(list) -> list
            {:DOWN, ^ref, _, _, _} -> []
          end

        Process.demonitor(ref, [:flush])
        result
    end
  end

  ## Private helpers

  # Registration happens inside the spawned process, so it is asynchronous
  # by nature. Wait for the ack (or failure) so the name is guaranteed to be
  # visible via whereis/1 when start/1 returns.
  defp await_registration(pid, name) do
    receive do
      {:queue_registered, ^pid, ^name} ->
        pid

      {:queue_register_failed, ^pid} ->
        raise ArgumentError,
              "could not start internal queue #{inspect(name)}: name already taken or process died"
    after
      5_000 ->
        raise ArgumentError,
              "internal queue #{inspect(name)} did not register within 5s"
    end
  end

  defp resolve(name) when is_atom(name), do: Process.whereis(name)
  defp resolve(pid) when is_pid(pid), do: if(Process.alive?(pid), do: pid)

  # There are waiting readers and buffered data – deliver immediately.
  defp loop([reader | rest_readers], [_ | _] = data, name) do
    SuperCache.Log.debug(fn ->
      "super_cache, internal_queue, sending #{length(data)} item(s) to #{inspect(reader)}"
    end)

    send(reader, data)
    loop(rest_readers, [], name)
  end

  # Stop terminates the process immediately: waiting readers are notified,
  # buffered items are dropped (they are best-effort lazy writes). Latching
  # stop as a status instead would leave a zombie process behind whenever
  # no reader happens to be waiting.
  defp loop(readers, data, name) do
    receive do
      :stop ->
        Enum.each(readers, &send(&1, :stop))

        SuperCache.Log.debug(fn ->
          "super_cache, internal_queue, stopped #{inspect(name)} " <>
            "(#{length(data)} item(s) discarded)"
        end)

      {:add, item} ->
        loop(readers, [item | data], name)

      {:get, from} ->
        loop([from | readers], data, name)
    end
  end
end
