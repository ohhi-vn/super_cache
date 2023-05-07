defmodule SuperCache.Partition.Utils do
  @moduledoc """
  Documentation for `SuperCache`.
  """

  def get_hash(term) do
    :erlang.phash2(term)
  end

  def get_pattition(term, range) do
    :erlang.phash2(term, range)
  end
end
