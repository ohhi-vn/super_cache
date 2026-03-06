defmodule SuperCache.Internal.Queue do
  @moduledoc false

  require Logger

  ## API

  @spec start(atom()) :: pid()
  def start(name) do
    spawn(fn ->
      Process.register(self(), name)
      loop([], [], :run, name)
    end)
  end

  @spec add(atom() | pid(), any()) :: :ok
  def add(pid, data) do
    send(pid, {:add, data})
    :ok
  end

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
    Logger.debug(fn -> "super_cache, internal.queue, sending #{length(data)} item(s) to #{inspect(reader)}" end)
    send(reader, data)
    loop(rest_readers, [], status, name)
  end

  # There are waiting readers but no data, and we are stopping – notify them.
  defp loop([_ | _] = readers, [], :stop, name) do
    Enum.each(readers, &send(&1, :stop))
    Logger.debug(fn -> "super_cache, internal.queue, #{inspect(name)} stopped" end)
  end

  defp loop(readers, data, status, name) do
    receive do
      {:add, item} -> loop(readers, [item | data], status, name)
      {:get, from} -> loop([from | readers], data, status, name)
      :stop -> loop(readers, data, :stop, name)
    end
  end
end
