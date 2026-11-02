defmodule SuperCache.DistributedWriteFailureTest do
  @moduledoc """
  Verifies that replication failures reach callers through every public write
  surface: bang APIs raise `SuperCache.Cluster.Error`, non-bang APIs return
  `{:error, exception}`, and no API reports success after a quorum failure.

  Remote replicas are simulated with non-connectable node names so the sync
  quorum fails without starting peers.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Error, Manager}
  alias SuperCache.{Config, Partition, Struct}

  defmodule FailureStruct do
    defstruct [:id, :data]
  end

  @pt_partition_map {Manager, :partition_map}

  setup do
    SuperCache.stop()
    Process.sleep(30)
    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)

    original_map = :persistent_term.get(@pt_partition_map, nil)
    Struct.init(%FailureStruct{}, :id)

    Config.set_config(:cluster, :distributed)
    Config.set_config(:replication_mode, :sync)

    on_exit(fn ->
      Config.set_config(:cluster, :local)
      Config.set_config(:replication_mode, :async)

      if original_map,
        do: :persistent_term.put(@pt_partition_map, original_map),
        else: :persistent_term.erase(@pt_partition_map)
    end)

    {:ok, original_map: original_map}
  end

  defp fail_partition(order) do
    :persistent_term.put(@pt_partition_map, %{order => {node(), [:write_failure_dead@host]}})
    order
  end

  defp fail_read_partition(order) do
    :persistent_term.put(@pt_partition_map, %{order => {:write_failure_dead@host, []}})
    order
  end

  test "SuperCache bang write raises and non-bang write returns an error" do
    key = :"wfail_super_#{System.unique_integer([:positive])}"
    fail_partition(Partition.get_partition_order(key))

    assert_raise Error, fn -> SuperCache.put!({key, 1}) end
    assert {:error, %Error{}} = SuperCache.put({key, 1})
  end

  test "KeyValue.add returns the documented error tuple" do
    kv_name = "wfail_kv_#{System.unique_integer([:positive])}"
    fail_partition(Partition.get_partition_order(kv_name))

    assert {:error, %Error{}} = SuperCache.KeyValue.add(kv_name, :key, :value)
  end

  test "Queue.add returns the documented error tuple" do
    queue_name = "wfail_queue_#{System.unique_integer([:positive])}"
    fail_partition(Partition.get_partition_order(queue_name))

    assert {:error, %Error{}} = SuperCache.Queue.add(queue_name, :job)
  end

  test "Stack.push returns the documented error tuple" do
    stack_name = "wfail_stack_#{System.unique_integer([:positive])}"
    fail_partition(Partition.get_partition_order(stack_name))

    assert {:error, %Error{}} = SuperCache.Stack.push(stack_name, :item)
  end

  test "Struct.add returns the documented error tuple" do
    struct = %FailureStruct{id: System.unique_integer([:positive]), data: :value}
    namespace = {:struct_storage, FailureStruct}
    fail_partition(Partition.get_partition_order(namespace))

    assert {:error, %Error{}} = Struct.add(struct)
  end

  test "SuperCache bang read raises and non-bang read returns an error" do
    key = :"wfail_read_super_#{System.unique_integer([:positive])}"
    fail_read_partition(Partition.get_partition_order(key))

    assert_raise Error, fn -> SuperCache.get!({key, nil}, read_mode: :primary) end
    assert {:error, %Error{}} = SuperCache.get({key, nil}, read_mode: :primary)
  end

  test "KeyValue.get returns the documented error tuple" do
    kv_name = "wfail_read_kv_#{System.unique_integer([:positive])}"
    fail_read_partition(Partition.get_partition_order(kv_name))

    assert {:error, %Error{}} = SuperCache.KeyValue.get(kv_name, :key, nil, read_mode: :primary)
  end

  test "Queue.peak returns the documented error tuple" do
    queue_name = "wfail_read_queue_#{System.unique_integer([:positive])}"
    fail_read_partition(Partition.get_partition_order(queue_name))

    assert {:error, %Error{}} = SuperCache.Queue.peak(queue_name, nil, read_mode: :primary)
  end

  test "Stack.count returns the documented error tuple" do
    stack_name = "wfail_read_stack_#{System.unique_integer([:positive])}"
    fail_read_partition(Partition.get_partition_order(stack_name))

    assert {:error, %Error{}} = SuperCache.Stack.count(stack_name, read_mode: :primary)
  end

  test "Struct.get returns the documented error tuple" do
    struct = %FailureStruct{id: System.unique_integer([:positive])}
    namespace = {:struct_storage, FailureStruct}
    fail_read_partition(Partition.get_partition_order(namespace))

    assert {:error, %Error{}} = Struct.get(struct, read_mode: :primary)
  end
end
