defmodule SuperCache.StructTest do
  use ExUnit.Case, async: false
  doctest SuperCache

  alias SuperCache.Struct

  defmodule MyStruct do
    defstruct [:id, :data]
  end

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!()
    :ok
  end

  setup do
    SuperCache.delete_all()
    Struct.init(%MyStruct{}, :id)
    :ok
  end

  test "add & get data" do
    data = %MyStruct{id: 1, data: :a}
    Struct.add(data)
    {:ok, result} = Struct.get(%MyStruct{id: 1})
    assert data == result
  end

  test "add overwrite old data" do
    data1 = %MyStruct{id: 1, data: :a}
    Struct.add(data1)
    data2 = %MyStruct{id: 1, data: :b}
    Struct.add(data2)

    {:ok, result} = Struct.get(%MyStruct{id: 1})
    assert data2 == result
  end

  test "remove data" do
    data = %MyStruct{id: 1, data: :a}
    Struct.add(data)
    Struct.remove(data)
    result = Struct.get(%MyStruct{id: 1})

    assert match?({:error, _}, result)
  end

  test "remove all data" do
    data1 = %MyStruct{id: 1, data: :a}
    Struct.add(data1)
    data2 = %MyStruct{id: 2, data: :b}
    Struct.add(data2)
    Struct.remove_all(%MyStruct{})
    {:ok, result} = Struct.get_all(%MyStruct{})
    assert [] == result
  end
end
