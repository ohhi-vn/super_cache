defmodule SuperCache.Queue do

  require Logger

  ## API

  def start(name) do
    spawn(fn ->
      Process.register(self(), name)
      loop([], [])
    end)
   end

  @spec add(atom | pid | port | reference | {atom, atom}, any) :: any
  @doc """
  receives data then store in queue.
  """
  def add(pid, data) do
    send(pid, {:add, data})
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

  defp loop([pid|wait], [_|_] = data_list) do
    Logger.debug("send #{inspect data_list} for #{inspect pid}")
    send(pid, data_list)
    loop(wait, [])
  end

  defp loop(wait_list, data_list) do
    receive do
      {:add, data} ->
        loop(wait_list, [data|data_list])
      {:get, from} ->
        loop([from|wait_list], data_list)
    end
  end
end
