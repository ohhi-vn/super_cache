defmodule SuperCache.EtsHolderTest do
  use ExUnit.Case

  setup_all do
    {:ok, _} = SuperCache.EtsHolder.start_link(__MODULE__)
    :ok
  end

  test "insert & get data" do
    :ets.insert(__MODULE__, {:key, "test insert data"})
    assert([{:key, "test insert data"}] == :ets.lookup(__MODULE__, :key))
  end

  test "test clean data" do
    :ets.insert(__MODULE__, {:key, "test insert data"})
    SuperCache.EtsHolder.clean(__MODULE__)
    assert([] == :ets.lookup(__MODULE__, :key))
  end
end
