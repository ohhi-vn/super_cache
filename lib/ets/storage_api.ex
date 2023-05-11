defmodule SuperCache.Storage do
  require Logger

  alias  SuperCache.{Sup, EtsHolder}
  alias :ets, as: Ets

  def start(num) when is_integer(num) and (num > 0) do
    workers =
      Enum.reduce(0..num-1, [], fn (el, result) ->
        name = String.to_atom("supercache_partition_#{el}")
        [{EtsHolder, name} | result]
      end )
    Logger.debug("start storage workers: #{inspect workers}")

    Sup.start_worker(workers)
  end

  def stop(num) when is_integer(num) and (num > 0) do
    Logger.debug("stop storage workers (#{num})")

    Enum.each(0..num-1, fn (el) ->
      name = String.to_atom("supercache_partition_#{el}")
      Logger.debug("stop storage workers #{inspect name}")
      EtsHolder.stop(name)
    end )
  end

  def put(term, partition) do
    Ets.insert(partition, term)
  end

  def get(key, partition) do
    Ets.lookup(partition, key)
  end

  def get_by_match(pattern, partition) do
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match(partition, pattern)
  end

  def get_by_match_object(pattern, partition) do
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match_object(partition, pattern)
  end

  def scan(fun, acc, partition) do
    Ets.foldl(fun, acc, partition)
  end

  def delete(key, partition) do
    Ets.delete(partition, key)
  end

  def delete_match(pattern, partition) do
    Logger.debug("storage, pattern for match: #{inspect pattern}, partition: #{partition}")
    Ets.match_delete(partition, pattern)
  end
end
