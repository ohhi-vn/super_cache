defmodule SuperCache.EtsHolderTest do
  use ExUnit.Case

  alias SuperCache.EtsHolder

  setup_all do
    {:ok, _} = EtsHolder.start_link(__MODULE__)
    EtsHolder.new_table(__MODULE__, __MODULE__)
    :ok
  end

  setup do
    EtsHolder.clean_all(__MODULE__)
  end

  test "insert & get data" do
    :ets.insert(__MODULE__, {:key, "test insert data"})
    assert([{:key, "test insert data"}] == :ets.lookup(__MODULE__, :key))
  end

  test "test clean data" do
    :ets.insert(__MODULE__, {:key, "test insert data"})
    SuperCache.EtsHolder.clean(__MODULE__, __MODULE__)
    assert([] == :ets.lookup(__MODULE__, :key))
  end
end
