defmodule SuperCache.Storage do
  require Logger

  alias  SuperCache.Sup
  alias SuperCache.EtsHolder
  alias :ets, as: Ets

  def start(num) when is_integer(num) and (num > 0) do
    workers =
      Enum.reduce(0..num-1, [], fn (el, result) ->
        name = String.to_atom("partition_#{el}")
        [{EtsHolder, name} | result]
      end )
    Logger.debug("start storage workers: #{inspect workers}")

    Sup.start_worker(workers)
  end

  def stop(num) when is_integer(num) and (num > 0) do
    Logger.debug("stop storage worker")

    Enum.each(1..num, fn (el) ->
      name = String.to_atom("partition_#{el}")
      Logger.debug("stop storage workers: #{inspect name}")
      EtsHolder.stop(name)
    end )
  end

  def put(term, partition) do
    Ets.insert(partition, term)
  end

  def get(key, partition) do
    Ets.lookup(partition, key)
  end
end
