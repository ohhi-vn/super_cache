defmodule SuperCache.Internal.Stream do
  @moduledoc false

  require Logger
  require SuperCache.Log

  alias SuperCache.Internal.Queue

  def create(q) do
    Stream.resource(
      fn ->
        q
      end,
      fn queue ->
        SuperCache.Log.debug(fn -> "get data from queue" end)
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
    |> Stream.run()
  end

  def push(data) do
    SuperCache.put(data)
    SuperCache.Log.debug(fn -> "pushed #{inspect(data)} to cache" end)
  end
end
