defmodule SuperCache.Distributed.StackTest do
  use ExUnit.Case, async: false
  alias SuperCache.Distributed.Stack

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

    :ok
  end

  setup do
    SuperCache.Distributed.delete_all()
    :ok
  end

  ## push / pop ─────────────────────────────────────────────────────────────────

  test "push and pop single item" do
    Stack.push("s", "hello")
    assert "hello" == Stack.pop("s")
  end

  test "pop returns default when stack is empty" do
    assert nil == Stack.pop("s")
    assert :empty == Stack.pop("s", :empty)
  end

  test "pop returns default when stack does not exist" do
    assert nil == Stack.pop("no_such_stack")
  end

  test "LIFO order is maintained" do
    Enum.each(1..10, &Stack.push("s", &1))
    results = Enum.map(1..10, fn _ -> Stack.pop("s") end)
    assert Enum.to_list(10..1//-1) == results
  end

  test "interleaved push and pop preserves LIFO order" do
    Stack.push("s", 1)
    Stack.push("s", 2)
    assert 2 == Stack.pop("s")
    Stack.push("s", 3)
    assert 3 == Stack.pop("s")
    assert 1 == Stack.pop("s")
    assert nil == Stack.pop("s")
  end

  ## multiple stacks ─────────────────────────────────────────────────────────────

  test "multiple stacks are independent" do
    Enum.each(1..5, fn x ->
      Stack.push("sa", x)
      Stack.push("sb", x * 10)
    end)

    result =
      Enum.reduce(5..1//-1, true, fn x, acc ->
        acc and Stack.pop("sa") == x and Stack.pop("sb") == x * 10
      end)

    assert result
  end

  ## count ──────────────────────────────────────────────────────────────────────

  test "count reflects stack size" do
    Enum.each(1..10, &Stack.push("s", &1))
    assert 10 == Stack.count("s")
  end

  test "count returns 0 for empty stack" do
    assert 0 == Stack.count("s")
  end

  test "count returns 0 for non-existent stack" do
    assert 0 == Stack.count("ghost_stack")
  end

  test "count decreases after pop" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.pop("s")
    assert 4 == Stack.count("s")
  end

  test "count after mixed push and pop" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.pop("s")
    Stack.pop("s")
    Stack.push("s", 99)
    assert 4 == Stack.count("s")
  end

  test "count with read_mode :primary" do
    Enum.each(1..3, &Stack.push("s_pm", &1))
    assert 3 == Stack.count("s_pm", read_mode: :primary)
  end

  test "count with read_mode :quorum" do
    Enum.each(1..3, &Stack.push("s_qm", &1))
    assert 3 == Stack.count("s_qm", read_mode: :quorum)
  end

  ## get_all (drain) ─────────────────────────────────────────────────────────────

  test "get_all returns items top-first and clears stack" do
    Enum.each(1..5, &Stack.push("s", &1))
    assert [5, 4, 3, 2, 1] == Stack.get_all("s")
    assert 0 == Stack.count("s")
  end

  test "get_all on empty stack returns empty list" do
    assert [] == Stack.get_all("s")
  end

  test "stack is usable after get_all" do
    Enum.each(1..5, &Stack.push("s", &1))
    Stack.get_all("s")
    Stack.push("s", :new)
    assert 1 == Stack.count("s")
    assert :new == Stack.pop("s")
  end

  ## replication_mode: :strong (3PC) ────────────────────────────────────────────

  test "push/pop survive under :strong replication_mode" do
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

    Enum.each(1..5, &Stack.push("strong_s", &1))
    assert 5 == Stack.count("strong_s", read_mode: :primary)
    assert 5 == Stack.pop("strong_s")
    assert 4 == Stack.count("strong_s", read_mode: :primary)

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
  end

  ## complex ────────────────────────────────────────────────────────────────────

  test "complex lifecycle" do
    Enum.each(1..10, &Stack.push("s", &1))
    assert 10 == Stack.count("s")

    assert 10 == Stack.pop("s")
    assert 9 == Stack.count("s")

    Stack.push("s", 100)
    assert 10 == Stack.count("s")
    assert 100 == Stack.pop("s")

    all = Stack.get_all("s")
    assert length(all) == 9
    assert 0 == Stack.count("s")

    Stack.push("s", :after_drain)
    assert 1 == Stack.count("s")
    assert :after_drain == Stack.pop("s")
    assert 0 == Stack.count("s")
  end
end
