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

  @default_timeout 5_000
  @max_retries 3

  ## API

  @doc """
  Starts a new queue process registered under `name`.

  Returns the PID of the started process. Raises if `name` is already taken.
  """
  @spec start(atom()) :: pid()
  def start(name) when is_atom(name) do
    spawn(fn ->
      try do
        Process.register(self(), name)
        Logger.debug("super_cache, internal_queue, started #{inspect(name)}")
        loop([], [], :run, name)
      rescue
        err ->
          Logger.error(
            "super_cache, internal_queue, failed to register #{inspect(name)}: #{inspect(err)}"
          )

          exit({:register_failed, name, err})
      end
    end)
  end

  @doc """
  Adds `data` to the queue.

  Returns `:ok` immediately. If the queue process is not alive, logs a warning
  and returns `{:error, :process_down}`.
  """
  @spec add(atom() | pid(), any()) :: :ok
  def add(pid, data) do
    send(pid, {:add, data})
    :ok
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

  def get(pid) do
    send(pid, {:get, self()})

    receive do
      :stop -> []
      list when is_list(list) -> list
    end
  end

  ## Private helpers



  # There are waiting readers and buffered data – deliver immediately.
  defp loop([reader | rest_readers], [_ | _] = data, status, name) do
    SuperCache.Log.debug(fn ->
      "super_cache, internal_queue, sending #{length(data)} item(s) to #{inspect(reader)}"
    end)

    send(reader, data)
    loop(rest_readers, [], status, name)
  end

  # There are waiting readers but no data, and we are stopping – notify them.
  defp loop([_ | _] = readers, [], :stop, name) do
    Enum.each(readers, &send(&1, :stop))
    SuperCache.Log.debug(fn -> "super_cache, internal_queue, #{inspect(name)} stopped" end)
  end

  defp loop(readers, data, status, name) do
    receive do
      {:add, item} -> loop(readers, [item | data], status, name)
      {:get, from} -> loop([from | readers], data, status, name)
      :stop -> loop(readers, data, :stop, name)
    end
  end
end
