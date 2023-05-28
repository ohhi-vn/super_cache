defmodule SuperCache.Queue do

  require Logger

  ## API

  def start(name) do
    spawn(fn ->
      Process.register(self(), name)
      loop([], [], :run, name)
    end)
   end

  @spec add(atom | pid | port | reference | {atom, atom}, any) :: any
  @doc """
  receives data then store in queue.
  """
  def add(pid, data) do
    send(pid, {:add, data})
    :ok
  end

  def stop(pid) do
    send(pid, :stop)
  end

  @doc """
  gets data from queue
  """
  def get(pid) do
    send(pid, {:get, self()})

    receive do
      data when is_tuple(data) ->
        [data]
      list ->
        list
    end
  end

  defp loop([pid|wait], [_|_] = data_list, status, name) do
    Logger.debug("send #{inspect data_list} for #{inspect pid}")
    send(pid, data_list)
    loop(wait, [], status, name)
  end
  defp loop([_|_] = pids, [], :stop, name) do
    for pid <- pids do
      Logger.debug("send stop for #{inspect pid}")
      send(pid, :stop)
    end
    Logger.debug("queue #{inspect name} is stopped")
  end
  defp loop(wait_list, data_list, status, name) do
    receive do
      {:add, data} ->
        loop(wait_list, [data|data_list], status, name)
      {:get, from} ->
        loop([from|wait_list], data_list, status, name)
      :stop ->
        loop(wait_list, data_list, :stop, name)
    end
  end
end
