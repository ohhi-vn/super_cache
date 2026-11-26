defmodule SuperCache.Cluster.Raft.MachineTest do
  use ExUnit.Case, async: false

  alias SuperCache.{Bootstrap, Partition, Storage}
  alias SuperCache.Cluster.Raft.{Applied, Machine}

  setup do
    if SuperCache.started?(), do: Bootstrap.stop()
    Process.sleep(30)

    :ok =
      Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        num_partition: 4,
        cluster: :local,
        replication_mode: :raft
      )

    partition_idx = 0
    Applied.reset(partition_idx)
    state = Machine.init(%{partition_idx: partition_idx})

    on_exit(fn ->
      if SuperCache.started?(), do: Bootstrap.stop()
      Applied.reset(partition_idx)
    end)

    %{state: state, partition_idx: partition_idx}
  end

  test "init keeps the configured partition and starts at index zero", %{state: state} do
    assert state == %{partition_idx: 0, applied: 0}
  end

  test "applies a put command to the local partition", %{state: state, partition_idx: idx} do
    record = {:raft_machine_key, "value"}
    partition = Partition.get_partition_by_idx(idx)

    assert {%{applied: 1}, :ok} = Machine.apply(%{index: 1}, {:put, record}, state)
    assert Storage.get(:raft_machine_key, partition) == [record]
  end

  test "checkpoint advances on success and ignores replayed indexes", %{state: state} do
    assert {%{applied: 3} = checkpointed, :ok} =
             Machine.apply(%{index: 3}, {:put, {:raft_checkpoint, 1}}, state)

    assert Applied.get(0) == 3

    assert {^checkpointed, {:ok, :already_applied}} =
             Machine.apply(%{index: 2}, {:put, {:raft_checkpoint, 2}}, checkpointed)

    assert Applied.get(0) == 3
  end

  test "a failed apply does not advance the checkpoint", %{partition_idx: idx} do
    bad_state = %{partition_idx: 999, applied: 0}

    assert {^bad_state, {:error, {:apply_failed, _}}} =
             Machine.apply(%{index: 20}, {:put, {:unknown_partition, 1}}, bad_state)

    assert bad_state.applied == 0
    assert Applied.get(idx) == 0
  end

  test "a cleared watermark replays an already applied increment", %{state: state} do
    partition = Partition.get_partition_by_idx(0)
    command = {:increment, {:raft_replay_counter, 0, 1}}

    assert {_state, :ok} = Machine.apply(%{index: 4}, command, state)
    assert Storage.get(:raft_replay_counter, partition) == [{:raft_replay_counter, 1}]

    Applied.reset(0)

    assert {_state, :ok} = Machine.apply(%{index: 4}, command, state)
    assert Storage.get(:raft_replay_counter, partition) == [{:raft_replay_counter, 2}]
  end

  test "installing a snapshot restores the applied watermark" do
    Applied.reset(0)

    assert [] =
             Machine.snapshot_installed(
               %{index: 7},
               %{partition_idx: 0, applied: 7},
               %{},
               %{partition_idx: 0, applied: 0}
             )

    assert Applied.get(0) == 7
  end

  test "a replayed index does not re-apply an increment", %{state: state, partition_idx: idx} do
    partition = Partition.get_partition_by_idx(idx)

    {state, reply} = Machine.apply(%{index: 1}, {:increment, {:raft_counter, 0, 1}}, state)
    assert reply == :ok
    assert Storage.get(:raft_counter, partition) == [{:raft_counter, 1}]

    {_state, reply} = Machine.apply(%{index: 1}, {:increment, {:raft_counter, 0, 1}}, state)
    assert reply == {:ok, :already_applied}
    assert Storage.get(:raft_counter, partition) == [{:raft_counter, 1}]

    {_state, _reply} = Machine.apply(%{index: 2}, {:increment, {:raft_counter, 0, 1}}, state)
    assert Storage.get(:raft_counter, partition) == [{:raft_counter, 2}]
  end

  test "a failing local apply is returned as an error" do
    bad_state = %{partition_idx: 999}

    assert {^bad_state, {:error, {:apply_failed, _reason}}} =
             Machine.apply(%{index: 20}, {:put, {:unknown_partition, 1}}, bad_state)
  end

  test "a malformed batch is returned as an error", %{state: state} do
    assert {^state, {:error, {:apply_failed, _reason}}} =
             Machine.apply(%{index: 21}, {:batch, [:not_an_operation]}, state)
  end

  test "applies a batch as one committed operation", %{state: state, partition_idx: idx} do
    partition = Partition.get_partition_by_idx(idx)

    ops = [
      {:put, {:batch_a, 1}},
      {:put, {:batch_b, 2}},
      {:delete, :batch_a}
    ]

    assert {_state, :ok} = Machine.apply(%{index: 5}, {:batch, ops}, state)
    assert Storage.get(:batch_a, partition) == []
    assert Storage.get(:batch_b, partition) == [{:batch_b, 2}]
  end
end
