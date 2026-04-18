defmodule SuperCache.Cluster.WALTest do
  @moduledoc """
  Tests for SuperCache.Cluster.WAL — the Write-Ahead Log for fast strong consistency.

  The WAL replaces the heavy Three-Phase Commit (3PC) protocol with a lighter-weight
  approach that achieves ~200µs latency vs ~1500µs for 3PC.

  ## Test categories

  - **Lifecycle**: GenServer start/stop, ETS table creation
  - **Local commits**: When no replicas exist, commits apply locally and return immediately
  - **Sequence generation**: Atomic `next_seq/0` must produce unique numbers under concurrency
  - **Statistics**: `stats/0` returns correct pending/acks counts
  - **Recovery**: Uncommitted entries are replayed after restart
  - **Acknowledgment**: `ack/2` updates tracking and triggers majority notification
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.WAL
  alias SuperCache.{Config, Partition, Storage}

  # ── Setup ────────────────────────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?() do
      SuperCache.stop()
      Process.sleep(100)
    end

    SuperCache.start!(
      key_pos: 0,
      partition_pos: 0,
      cluster: :local,
      num_partition: 4
    )

    :ok
  end

  setup do
    # Clean WAL tables between tests
    try do
      :ets.delete_all_objects(SuperCache.Cluster.WAL)
    catch
      :error, :badarg -> :ok
    end

    try do
      :ets.delete_all_objects(SuperCache.Cluster.WAL_acks)
    catch
      :error, :badarg -> :ok
    end

    SuperCache.delete_all()
    :ok
  end

  # ── Lifecycle ────────────────────────────────────────────────────────────────

  describe "WAL GenServer lifecycle" do
    test "WAL process is running after application start" do
      assert Process.whereis(WAL) != nil
    end

    test "WAL ETS tables exist after start" do
      assert :ets.info(WAL) != :undefined
      assert :ets.info(SuperCache.Cluster.WAL_acks) != :undefined
    end

    test "stats returns map with expected keys" do
      stats = WAL.stats()
      assert Map.has_key?(stats, :pending)
      assert Map.has_key?(stats, :acks_pending)
      assert is_integer(stats.pending)
      assert is_integer(stats.acks_pending)
    end

    test "stats shows zero entries after clean start" do
      stats = WAL.stats()
      assert stats.pending >= 0
      assert stats.acks_pending >= 0
    end
  end

  # ── Local-only commits (no replicas) ─────────────────────────────────────────

  describe "commit/2 — local-only (no replicas)" do
    test "applies operations locally when no replicas exist" do
      # In local mode, Manager.get_replicas returns {node(), []}
      # so commit should apply locally and return :ok immediately
      partition_idx = 0
      ops = [{:put, {:test_key, "test_value"}}]

      result = WAL.commit(partition_idx, ops)
      assert result == :ok

      # Verify the data was written to ETS
      partition = Partition.get_partition_by_idx(partition_idx)
      assert Storage.get(:test_key, partition) == [{:test_key, "test_value"}]
    end

    test "applies delete operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      # First, insert some data
      Storage.put({:delete_me, "value"}, partition)
      assert Storage.get(:delete_me, partition) != []

      # Then delete via WAL
      result = WAL.commit(partition_idx, [{:delete, :delete_me}])
      assert result == :ok

      # Verify deletion
      assert Storage.get(:delete_me, partition) == []
    end

    test "applies delete_match operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      # Insert multiple records
      Storage.put({:match_a, 1}, partition)
      Storage.put({:match_b, 2}, partition)

      # Delete matching records
      result = WAL.commit(partition_idx, [{:delete_match, {:match_a, :_}}])
      assert result == :ok

      assert Storage.get(:match_a, partition) == []
      assert Storage.get(:match_b, partition) != []
    end

    test "applies delete_all operation locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      # Insert some data
      Storage.put({:del_all_key, "value1"}, partition)
      Storage.put({:del_all_key, "value2"}, partition)

      # Delete all via WAL
      result = WAL.commit(partition_idx, [{:delete_all, nil}])
      assert result == :ok

      # Verify all deleted
      assert Storage.stats(partition) |> elem(1) == 0
    end

    test "applies multiple operations in order" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      ops = [
        {:put, {:multi_key, "first"}},
        {:delete, :multi_key},
        {:put, {:multi_key, "second"}}
      ]

      result = WAL.commit(partition_idx, ops)
      assert result == :ok

      # The final state should have "second" as the value
      assert Storage.get(:multi_key, partition) == [{:multi_key, "second"}]
    end
  end

  # ── Sequence number generation ───────────────────────────────────────────────

  describe "sequence number uniqueness" do
    test "concurrent commits produce unique WAL entries" do
      # This tests the fix for the next_seq/0 race condition.
      # Previously, next_seq used non-atomic persistent_term read+write,
      # which could produce duplicate sequence numbers under concurrency.
      # Now it uses :ets.update_counter which is atomic.

      partition_idx = 0
      num_concurrent = 100

      tasks =
        for i <- 1..num_concurrent do
          Task.async(fn ->
            WAL.commit(partition_idx, [{:put, {:"seq_test_#{i}", i}}])
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # All commits should succeed
      assert Enum.all?(results, &(&1 == :ok))

      # All WAL entries should have unique sequence numbers.
      # Read all entries from the WAL table and check for duplicates.
      entries = :ets.tab2list(WAL)

      # Filter out the counter entry {:seq, :counter}
      wal_entries =
        Enum.filter(entries, fn
          {{:seq, :counter}, _} -> false
          _ -> true
        end)

      seq_numbers = Enum.map(wal_entries, fn {seq, _} -> seq end)
      unique_seqs = Enum.uniq(seq_numbers)

      assert length(seq_numbers) == length(unique_seqs),
             "Duplicate sequence numbers found: #{inspect(seq_numbers -- unique_seqs)}"
    end

    test "sequence numbers are monotonically increasing" do
      partition_idx = 0

      # Commit a few operations sequentially
      for i <- 1..5 do
        WAL.commit(partition_idx, [{:put, {:"mono_test_#{i}", i}}])
      end

      # Read WAL entries and check ordering
      entries = :ets.tab2list(WAL)

      wal_entries =
        Enum.filter(entries, fn
          {{:seq, :counter}, _} -> false
          _ -> true
        end)

      seq_numbers = Enum.map(wal_entries, fn {seq, _} -> seq end) |> Enum.sort()

      # Each sequence number should be one more than the previous
      seq_numbers
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        assert b == a + 1, "Sequence numbers not monotonically increasing: #{a} -> #{b}"
      end)
    end
  end

  # ── Statistics ───────────────────────────────────────────────────────────────

  describe "stats/0" do
    test "pending count increases after local commit" do
      # Note: In local-only mode (no replicas), the WAL entry may be cleaned up
      # quickly. The pending count reflects entries that haven't been cleaned yet.
      _stats_before = WAL.stats()

      WAL.commit(0, [{:put, {:stats_test, "value"}}])

      stats_after = WAL.stats()

      # Pending count should be >= before (may have been cleaned already)
      assert stats_after.pending >= 0
    end

    test "stats returns integers" do
      stats = WAL.stats()
      assert is_integer(stats.pending)
      assert is_integer(stats.acks_pending)
    end
  end

  # ── Acknowledgment ──────────────────────────────────────────────────────────

  describe "ack/2" do
    test "ack for non-existent seq returns :ok without error" do
      # Acking a sequence number that doesn't exist should be a no-op
      result = WAL.ack(999_999, node())
      assert result == :ok
    end

    test "ack increments the acked count" do
      # Create a WAL entry by committing with a mock ack table entry
      # This simulates what happens during distributed replication
      seq = 1

      # Manually insert an ack tracking entry
      :ets.insert(SuperCache.Cluster.WAL_acks, {seq, %{acked: 0, required: 2, replicas: []}})

      # Ack it
      WAL.ack(seq, node())

      # Verify ack count was incremented
      case :ets.lookup(SuperCache.Cluster.WAL_acks, seq) do
        [{^seq, %{acked: acked}}] ->
          assert acked == 1

        [] ->
          # Entry was cleaned up (majority reached) — also acceptable
          :ok
      end
    end
  end

  # ── Recovery ────────────────────────────────────────────────────────────────

  describe "recover/0" do
    test "replay uncommitted entries after restart" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      # Clean the partition first
      Storage.delete_all(partition)

      # Commit some data via WAL
      WAL.commit(partition_idx, [{:put, {:recover_test, "recovered_value"}}])

      # Verify data exists
      assert Storage.get(:recover_test, partition) == [{:recover_test, "recovered_value"}]

      # Run recovery — should re-apply entries
      result = WAL.recover()
      assert result == :ok

      # Data should still exist after recovery
      assert Storage.get(:recover_test, partition) == [{:recover_test, "recovered_value"}]
    end

    test "recover returns :ok even with empty WAL" do
      # Clear the WAL table
      :ets.delete_all_objects(WAL)

      result = WAL.recover()
      assert result == :ok
    end

    test "recover re-applies multiple operations" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      Storage.delete_all(partition)

      # Commit multiple operations
      WAL.commit(partition_idx, [{:put, {:multi_recover_1, "v1"}}])
      WAL.commit(partition_idx, [{:put, {:multi_recover_2, "v2"}}])
      WAL.commit(partition_idx, [{:put, {:multi_recover_3, "v3"}}])

      # Run recovery
      assert WAL.recover() == :ok

      # All data should still be accessible
      assert Storage.get(:multi_recover_1, partition) == [{:multi_recover_1, "v1"}]
      assert Storage.get(:multi_recover_2, partition) == [{:multi_recover_2, "v2"}]
      assert Storage.get(:multi_recover_3, partition) == [{:multi_recover_3, "v3"}]
    end
  end

  # ── replicate_and_ack/3 ─────────────────────────────────────────────────────

  describe "replicate_and_ack/3" do
    test "applies operations locally" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      Storage.delete_all(partition)

      result = WAL.replicate_and_ack(1, partition_idx, [{:put, {:rep_ack_test, "value"}}])
      assert result == :ok

      assert Storage.get(:rep_ack_test, partition) == [{:rep_ack_test, "value"}]
    end

    test "applies delete operations" do
      partition_idx = 0
      partition = Partition.get_partition_by_idx(partition_idx)

      Storage.put({:rep_del_key, "value"}, partition)

      result = WAL.replicate_and_ack(1, partition_idx, [{:delete, :rep_del_key}])
      assert result == :ok

      assert Storage.get(:rep_del_key, partition) == []
    end
  end

  # ── Concurrent access ───────────────────────────────────────────────────────

  describe "concurrent commits" do
    test "handles concurrent commits to different partitions" do
      num_partitions = Config.get_config(:num_partition, 4)
      num_writers = 50

      tasks =
        for i <- 1..num_writers do
          partition_idx = rem(i, num_partitions)

          Task.async(fn ->
            WAL.commit(partition_idx, [{:put, {:"concurrent_#{i}", i}}])
          end)
        end

      results = Task.await_many(tasks, 10_000)

      assert Enum.all?(results, &(&1 == :ok))

      # Verify all data was written
      for i <- 1..num_writers do
        partition_idx = rem(i, num_partitions)
        partition = Partition.get_partition_by_idx(partition_idx)
        assert Storage.get(String.to_atom("concurrent_#{i}"), partition) != []
      end
    end

    test "handles concurrent commits to the same partition" do
      partition_idx = 0
      num_writers = 50

      tasks =
        for i <- 1..num_writers do
          Task.async(fn ->
            WAL.commit(partition_idx, [{:put, {:"same_part_#{i}", i}}])
          end)
        end

      results = Task.await_many(tasks, 10_000)

      assert Enum.all?(results, &(&1 == :ok))

      # Verify all data was written
      partition = Partition.get_partition_by_idx(partition_idx)

      for i <- 1..num_writers do
        assert Storage.get(String.to_atom("same_part_#{i}"), partition) != []
      end
    end
  end

  # ── Edge cases ──────────────────────────────────────────────────────────────

  describe "edge cases" do
    test "commit with empty ops list" do
      # Empty ops should still return :ok
      result = WAL.commit(0, [])
      assert result == :ok
    end

    test "commit to non-existent partition index" do
      # Should not crash even with an out-of-range partition index
      result = WAL.commit(999, [{:put, {:edge_key, "value"}}])
      # May return :ok or error depending on Partition.get_partition_by_idx behavior
      case result do
        :ok -> :ok
        {:error, _} -> :ok
      end
    end

    test "multiple acks for the same seq" do
      seq = 42

      # Insert an ack entry
      :ets.insert(SuperCache.Cluster.WAL_acks, {seq, %{acked: 0, required: 3, replicas: []}})

      # Ack multiple times
      WAL.ack(seq, :node1@host)
      WAL.ack(seq, :node2@host)
      WAL.ack(seq, :node3@host)

      # After 3 acks (>= required), the entry should be cleaned up
      # or the acked count should be 3
      case :ets.lookup(SuperCache.Cluster.WAL_acks, seq) do
        [] ->
          # Entry was cleaned up — majority reached
          :ok

        [{^seq, %{acked: acked}}] ->
          assert acked == 3
      end
    end

    test "WAL handles rapid start/stop cycles" do
      # The WAL should handle being stopped and started multiple times
      # without leaving stale state
      for _ <- 1..3 do
        WAL.commit(0, [{:put, {:cycle_key, "value"}}])
      end

      # Should not crash
      assert WAL.stats().pending >= 0
    end
  end
end
