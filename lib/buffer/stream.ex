defmodule SuperCache.Internal.Stream do

  require Logger

  alias SuperCache.Internal.Queue

  def create(q) do
    Stream.resource(
      fn ->
       q
      end,
      fn queue ->
        Logger.debug("get data from queue")
        {Queue.get(queue), queue}
      end,
      fn _queue ->
        :ok
      end
    )
  end

  def make_stream_pipe(enumable) do
    enumable
    |> Stream.each(&push/1)
    |> Stream.run
  end

  def push(data) do
    SuperCache.put(data)
    Logger.debug("pushed #{inspect data} to cache")
  end
end
