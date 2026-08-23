defmodule SuperCache.CoverageRoundTwoTest do
  @moduledoc """
  Final coverage round: EtsHolder as an independently started owner, Buffer
  drop-warning during queue restarts, and Queue spin-lock exhaustion guards.
  """

  use ExUnit.Case, async: false

  alias SuperCache.{Buffer, EtsHolder, Queue}

  # ── EtsHolder as a standalone owner ──────────────────────────────────────────

  describe "EtsHolder standalone instance" do
    test "start_link/1 owns tables that die with it" do
      name = :GapEtsHolderA
      {:ok, pid} = EtsHolder.start_link(name)
      assert Process.alive?(pid)

      assert :ok = EtsHolder.new_table(name, :gap_owned_table)
      assert :ets.info(:gap_owned_table) != :undefined

      assert :ok = EtsHolder.stop(name)
      Process.sleep(30)

      # terminate/2 deleted the tracked table.
      assert :ets.info(:gap_owned_table) == :undefined
    end

    test "clean_all/1 clears every tracked table" do
      name = :GapEtsHolderB
      {:ok, _} = EtsHolder.start_link(name)

      :ok = EtsHolder.new_table(name, :gap_ca_1)
      :ok = EtsHolder.new_table(name, :gap_ca_2)

      :ets.insert(:gap_ca_1, {:a, 1})
      :ets.insert(:gap_ca_2, {:b, 2})

      assert :ok = EtsHolder.clean_all(name)
      assert [] == :ets.tab2list(:gap_ca_1)
      assert [] == :ets.tab2list(:gap_ca_2)

      EtsHolder.stop(name)
    end

    test "delete_table/2 removes tracking and the table itself" do
      name = :GapEtsHolderC
      {:ok, _} = EtsHolder.start_link(name)

      :ok = EtsHolder.new_table(name, :gap_del_tbl)
      assert :ets.info(:gap_del_tbl) != :undefined

      assert :ok = EtsHolder.delete_table(name, :gap_del_tbl)
      assert :ets.info(:gap_del_tbl) == :undefined

      EtsHolder.stop(name)
    end
  end

  # ── Buffer: dropped writes while a buffer restarts ───────────────────────────

  test "enqueue warns and returns error when its queue is momentarily dead" do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
    Process.sleep(30)

    names = :persistent_term.get({Buffer, :buffer_names}, nil)
    assert is_tuple(names)
    victim = elem(names, 0)

    old_pid = Process.whereis(victim)
    Process.exit(old_pid, :kill)
    Process.sleep(5)

    # The runner has not recreated the queue yet — this enqueue must be
    # surfaced as dropped rather than vanishing silently.
    result = Buffer.enqueue({:dropped_probe, 1})
    assert result in [:ok, {:error, :process_down}]

    # The runner recreates the queue shortly after.
    wait_until(fn ->
      case Process.whereis(victim) do
        nil -> false
        pid -> pid != old_pid
      end
    end)
  end

  # ── Queue: spin-wait exhaustion guards ───────────────────────────────────────

  test "structural ops give up gracefully when the updating lock is stuck" do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
    Process.sleep(30)

    partition = SuperCache.Partition.get_partition("stuck_queue")
    qname = "stuck_queue"

    # Hold the initialisation lock forever.
    StorageProbe.put_updating(qname, partition)

    assert false == Queue.add(qname, :item)
    assert nil == Queue.out(qname)
    assert nil == Queue.peak(qname)
    assert 0 == Queue.count(qname)
    assert [] == Queue.get_all(qname)

    StorageProbe.delete_updating(qname, partition)
  end

  # ── helpers ──────────────────────────────────────────────────────────────────

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end
end

defmodule StorageProbe do
  alias SuperCache.Storage

  def put_updating(qname, partition) do
    Storage.put({{:queue, :updating, qname}, true}, partition)
  end

  def delete_updating(qname, partition) do
    Storage.delete({:queue, :updating, qname}, partition)
  end
end
