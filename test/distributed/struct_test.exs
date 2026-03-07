defmodule SuperCache.Distributed.StructTest do
  use ExUnit.Case, async: false
  alias SuperCache.Distributed.Struct, as: DStruct

  defmodule Person do
    defstruct [:id, :name, :age]
  end

  defmodule Product do
    defstruct [:sku, :name, :price]
  end

  setup_all do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(50)

    SuperCache.Cluster.Bootstrap.start!(
      key_pos: 0,
      partition_pos: 0,
      cluster: :distributed,
      replication_factor: 2,
      replication_mode: :async,
      num_partition: 3
    )

    DStruct.init(%Person{}, :id)
    DStruct.init(%Product{}, :sku)
    :ok
  end

  setup do
    DStruct.remove_all(%Person{})
    DStruct.remove_all(%Product{})
    :ok
  end

  ## init ───────────────────────────────────────────────────────────────────────

  test "init rejects a key that does not exist on the struct" do
    assert {:error, _} = DStruct.init(%Person{}, :nonexistent)
  end

  test "init rejects double initialisation" do
    assert {:error, "struct already initialised"} = DStruct.init(%Person{}, :id)
  end

  ## add / get ──────────────────────────────────────────────────────────────────

  test "add and get a struct" do
    person = %Person{id: 1, name: "Alice", age: 30}
    assert {:ok, ^person} = DStruct.add(person)
    assert {:ok, ^person} = DStruct.get(%Person{id: 1})
  end

  test "get returns not_found for missing key" do
    assert {:error, :not_found} = DStruct.get(%Person{id: 999})
  end

  test "add overwrites struct with same key" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    updated = %Person{id: 1, name: "Alice Updated", age: 31}
    DStruct.add(updated)
    assert {:ok, ^updated} = DStruct.get(%Person{id: 1})
  end

  test "different struct types are isolated" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    DStruct.add(%Product{sku: 1, name: "Widget", price: 9.99})
    assert {:ok, %Person{name: "Alice"}} = DStruct.get(%Person{id: 1})
    assert {:ok, %Product{name: "Widget"}} = DStruct.get(%Product{sku: 1})
  end

  test "multiple structs can be stored independently" do
    people = for i <- 1..5, do: %Person{id: i, name: "Person #{i}", age: 20 + i}
    Enum.each(people, &DStruct.add/1)

    Enum.each(people, fn p ->
      assert {:ok, ^p} = DStruct.get(%Person{id: p.id})
    end)
  end

  ## read_mode: :primary ────────────────────────────────────────────────────────

  test "get with read_mode :primary returns correct struct" do
    person = %Person{id: 10, name: "Bob", age: 25}
    DStruct.add(person)
    assert {:ok, ^person} = DStruct.get(%Person{id: 10}, read_mode: :primary)
  end

  test "get with read_mode :primary returns not_found for missing key" do
    assert {:error, :not_found} = DStruct.get(%Person{id: 9999}, read_mode: :primary)
  end

  test "get_all with read_mode :primary returns all structs" do
    people = for i <- 1..3, do: %Person{id: i, name: "P#{i}", age: i}
    Enum.each(people, &DStruct.add/1)
    {:ok, results} = DStruct.get_all(%Person{}, read_mode: :primary)
    assert length(results) == 3
    assert Enum.sort_by(results, & &1.id) == Enum.sort_by(people, & &1.id)
  end

  ## read_mode: :quorum ─────────────────────────────────────────────────────────

  test "get with read_mode :quorum returns correct struct" do
    person = %Person{id: 20, name: "Carol", age: 35}
    DStruct.add(person)
    assert {:ok, ^person} = DStruct.get(%Person{id: 20}, read_mode: :quorum)
  end

  test "get with read_mode :quorum returns not_found for missing key" do
    assert {:error, :not_found} = DStruct.get(%Person{id: 8888}, read_mode: :quorum)
  end

  test "get_all with read_mode :quorum returns all structs" do
    people = for i <- 1..4, do: %Person{id: i + 50, name: "QP#{i}", age: i}
    Enum.each(people, &DStruct.add/1)
    {:ok, results} = DStruct.get_all(%Person{}, read_mode: :quorum)
    ids = Enum.map(results, & &1.id) |> Enum.sort()
    assert Enum.map(people, & &1.id) |> Enum.sort() == ids
  end

  ## get_all ────────────────────────────────────────────────────────────────────

  test "get_all returns all stored structs" do
    people = for i <- 1..5, do: %Person{id: i, name: "Person #{i}", age: 20 + i}
    Enum.each(people, &DStruct.add/1)
    {:ok, results} = DStruct.get_all(%Person{})
    assert length(results) == 5
    assert Enum.sort_by(results, & &1.id) == Enum.sort_by(people, & &1.id)
  end

  test "get_all returns empty list when no structs stored" do
    assert {:ok, []} = DStruct.get_all(%Person{})
  end

  test "get_all is scoped to struct type" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    DStruct.add(%Product{sku: 10, name: "Widget", price: 5.0})
    {:ok, people} = DStruct.get_all(%Person{})
    {:ok, products} = DStruct.get_all(%Product{})
    assert length(people) == 1
    assert length(products) == 1
    assert hd(people).__struct__ == Person
    assert hd(products).__struct__ == Product
  end

  ## remove ─────────────────────────────────────────────────────────────────────

  test "remove deletes a struct by key" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    assert {:ok, _} = DStruct.remove(%Person{id: 1})
    assert {:error, :not_found} = DStruct.get(%Person{id: 1})
  end

  test "remove returns not_found for missing key" do
    assert {:error, :not_found} = DStruct.remove(%Person{id: 999})
  end

  test "remove only deletes the targeted struct" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    DStruct.add(%Person{id: 2, name: "Bob", age: 25})
    DStruct.remove(%Person{id: 1})
    assert {:error, :not_found} = DStruct.get(%Person{id: 1})
    assert {:ok, _} = DStruct.get(%Person{id: 2})
  end

  ## remove_all ─────────────────────────────────────────────────────────────────

  test "remove_all clears all structs of that type" do
    Enum.each(1..5, fn i -> DStruct.add(%Person{id: i, name: "P#{i}", age: i}) end)
    assert {:ok, :removed} = DStruct.remove_all(%Person{})
    assert {:ok, []} = DStruct.get_all(%Person{})
  end

  test "remove_all does not affect other struct types" do
    DStruct.add(%Person{id: 1, name: "Alice", age: 30})
    DStruct.add(%Product{sku: 1, name: "Widget", price: 1.0})
    DStruct.remove_all(%Person{})
    assert {:ok, []} = DStruct.get_all(%Person{})
    {:ok, products} = DStruct.get_all(%Product{})
    assert length(products) == 1
  end

  ## replication_mode: :strong (3PC) ────────────────────────────────────────────

  test "add and get survive under :strong replication_mode" do
    SuperCache.stop()
    Process.sleep(50)

    SuperCache.Cluster.Bootstrap.start!(
      key_pos: 0,
      partition_pos: 0,
      cluster: :distributed,
      replication_factor: 2,
      replication_mode: :strong,
      num_partition: 3
    )

    # Re-init after restart (struct key registration is lost on stop).
    DStruct.init(%Person{}, :id)

    person = %Person{id: 100, name: "Strong", age: 42}
    assert {:ok, ^person} = DStruct.add(person)
    assert {:ok, ^person} = DStruct.get(%Person{id: 100}, read_mode: :primary)
    assert {:ok, :removed} = DStruct.remove_all(%Person{})

    SuperCache.stop()
    Process.sleep(50)

    SuperCache.Cluster.Bootstrap.start!(
      key_pos: 0,
      partition_pos: 0,
      cluster: :distributed,
      replication_factor: 2,
      replication_mode: :async,
      num_partition: 3
    )

    DStruct.init(%Person{}, :id)
    DStruct.init(%Product{}, :sku)
  end

  ## complex ────────────────────────────────────────────────────────────────────

  test "complex lifecycle" do
    people = for i <- 1..10, do: %Person{id: i, name: "Person #{i}", age: 20 + i}
    Enum.each(people, &DStruct.add/1)

    {:ok, all} = DStruct.get_all(%Person{})
    assert length(all) == 10

    DStruct.add(%Person{id: 1, name: "Updated", age: 99})
    assert {:ok, %Person{name: "Updated", age: 99}} = DStruct.get(%Person{id: 1})

    assert {:ok, %Person{name: "Updated", age: 99}} =
             DStruct.get(%Person{id: 1}, read_mode: :primary)

    DStruct.remove(%Person{id: 5})
    assert {:error, :not_found} = DStruct.get(%Person{id: 5})
    {:ok, remaining} = DStruct.get_all(%Person{})
    assert length(remaining) == 9

    DStruct.remove_all(%Person{})
    assert {:ok, []} = DStruct.get_all(%Person{})

    DStruct.add(%Person{id: 1, name: "Phoenix", age: 1})
    assert {:ok, %Person{name: "Phoenix"}} = DStruct.get(%Person{id: 1})
    assert {:ok, %Person{name: "Phoenix"}} = DStruct.get(%Person{id: 1}, read_mode: :quorum)
  end
end
