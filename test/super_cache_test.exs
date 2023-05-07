defmodule SuperCacheTest do
  use ExUnit.Case
  doctest SuperCache

  test "greets the world" do
    assert SuperCache.hello() == :world
  end
end
