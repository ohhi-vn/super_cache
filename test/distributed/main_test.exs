defmodule SuperCache.DistributedTest do
  use ExUnit.Case, async: false

  alias SuperCache.Distributed, as: Cache

  # ── Setup ────────────────────────────────────────────────────────────────────

  setup_all do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(50)

    # key_pos: 1  → elem(tuple, 1) is the unique key  e.g. the numeric id
    # partition_pos: 0 → elem(tuple, 0) determines the partition  e.g. :user / :session
    SuperCache.Cluster.Bootstrap.start!(
      key_pos: 1,
      partition_pos: 0,
      cluster: :distributed,
      replication_factor: 2,
      replication_mode: :async,
      num_partition: 4
    )

    :ok
  end

  setup do
    Cache.delete_all()
    :ok
  end

  # ── put! / get! ───────────────────────────────────────────────────────────────

  describe "put!/1 and get!/2" do
    test "stores and retrieves a tuple" do
      Cache.put!({:user, 1, "Alice", :admin})
      assert [{:user, 1, "Alice", :admin}] == Cache.get!({:user, 1, nil, nil})
    end

    test "put! returns true" do
      assert true == Cache.put!({:item, "x", :eu, 1})
    end

    test "get! returns empty list when key not found" do
      assert [] == Cache.get!({:user, 999, nil, nil})
    end

    test "put! overwrites existing record with same key" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.put!({:user, 1, "Alice Updated", :superadmin})
      assert [{:user, 1, "Alice Updated", :superadmin}] == Cache.get!({:user, 1, nil, nil})
    end

    test "multiple records with different keys coexist" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.put!({:user, 2, "Bob", :member})
      assert [{:user, 1, "Alice", :admin}] == Cache.get!({:user, 1, nil, nil})
      assert [{:user, 2, "Bob", :member}] == Cache.get!({:user, 2, nil, nil})
    end

    test "records in different partitions coexist" do
      Cache.put!({:session, "tok-a", :eu, :active})
      Cache.put!({:session, "tok-b", :us, :active})
      assert [{:session, "tok-a", :eu, :active}] == Cache.get!({:session, "tok-a", nil, nil})
      assert [{:session, "tok-b", :us, :active}] == Cache.get!({:session, "tok-b", nil, nil})
    end
  end

  # ── put / get (non-bang) ──────────────────────────────────────────────────────

  describe "put/1 and get/2" do
    test "put returns true on success" do
      assert true == Cache.put({:user, 10, "Carol", :member})
    end

    test "get returns matching records" do
      Cache.put({:user, 10, "Carol", :member})
      assert [{:user, 10, "Carol", :member}] == Cache.get({:user, 10, nil, nil})
    end

    test "get returns empty list when not found" do
      assert [] == Cache.get({:user, 9999, nil, nil})
    end
  end

  # ── read_mode: :primary ───────────────────────────────────────────────────────

  describe "get!/2 with read_mode: :primary" do
    test "returns correct record from primary" do
      Cache.put!({:user, 1, "Alice", :admin})

      assert [{:user, 1, "Alice", :admin}] ==
               Cache.get!({:user, 1, nil, nil}, read_mode: :primary)
    end

    test "returns empty list when key not found on primary" do
      assert [] == Cache.get!({:user, 9999, nil, nil}, read_mode: :primary)
    end

    test "reflects overwrite immediately via primary" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.put!({:user, 1, "Alice v2", :superadmin})

      assert [{:user, 1, "Alice v2", :superadmin}] ==
               Cache.get!({:user, 1, nil, nil}, read_mode: :primary)
    end
  end

  # ── read_mode: :quorum ────────────────────────────────────────────────────────

  describe "get!/2 with read_mode: :quorum" do
    test "returns correct record via quorum" do
      Cache.put!({:user, 2, "Bob", :member})

      assert [{:user, 2, "Bob", :member}] ==
               Cache.get!({:user, 2, nil, nil}, read_mode: :quorum)
    end

    test "returns empty list when key not found via quorum" do
      assert [] == Cache.get!({:user, 8888, nil, nil}, read_mode: :quorum)
    end

    test "reflects overwrite via quorum" do
      Cache.put!({:user, 2, "Bob", :member})
      Cache.put!({:user, 2, "Bob v2", :admin})

      assert [{:user, 2, "Bob v2", :admin}] ==
               Cache.get!({:user, 2, nil, nil}, read_mode: :quorum)
    end
  end

  # ── get_by_key_partition! ─────────────────────────────────────────────────────

  describe "get_by_key_partition!/3" do
    test "retrieves by explicit key and partition" do
      Cache.put!({:user, 42, "Dave", :member})
      # key_pos: 1 → key = 42; partition_pos: 0 → partition val = :user
      assert [{:user, 42, "Dave", :member}] == Cache.get_by_key_partition!(42, :user)
    end

    test "returns empty list when key absent" do
      assert [] == Cache.get_by_key_partition!(9999, :user)
    end

    test "local and primary read modes agree" do
      Cache.put!({:user, 42, "Dave", :member})
      local = Cache.get_by_key_partition!(42, :user, read_mode: :local)
      primary = Cache.get_by_key_partition!(42, :user, read_mode: :primary)
      assert local == primary
    end

    test "quorum read mode returns the same result" do
      Cache.put!({:user, 42, "Dave", :member})
      quorum = Cache.get_by_key_partition!(42, :user, read_mode: :quorum)
      assert [{:user, 42, "Dave", :member}] == quorum
    end
  end

  describe "get_by_key_partition/3 non-bang" do
    test "returns list on success" do
      Cache.put!({:user, 55, "Eva", :admin})
      assert [{:user, 55, "Eva", :admin}] == Cache.get_by_key_partition(55, :user)
    end

    test "returns empty list for a partition that holds no matching key" do
      # nil is a valid partition value that hashes to some ETS table;
      # looking up a key that was never inserted returns [].
      assert [] == Cache.get_by_key_partition(:nonexistent_key_xyz, nil)
    end
  end

  # ── get_same_key_partition! ───────────────────────────────────────────────────

  describe "get_same_key_partition!/2" do
    test "retrieves when key_pos == partition_pos" do
      # Restart with key_pos == partition_pos == 0 to test the convenience fn.
      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :async,
        num_partition: 4
      )

      Cache.put!({:config, :timeout, 5_000})
      assert [{:config, :timeout, 5_000}] == Cache.get_same_key_partition!(:config)

      # Restore original config for remaining tests.
      Cache.delete_all()
      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 1,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :async,
        num_partition: 4
      )
    end

    test "returns empty list when key is absent" do
      assert [] == Cache.get_same_key_partition!(:nonexistent_key_xyz)
    end
  end

  # ── get_by_match! ─────────────────────────────────────────────────────────────

  describe "get_by_match!/3" do
    test "returns binding lists for a specific partition" do
      Cache.put!({:order, "o-1", :eu, :pending})
      Cache.put!({:order, "o-2", :eu, :shipped})
      # partition_pos: 0 → partition val = :order for all order tuples
      bindings = Cache.get_by_match!(:order, {:order, :"$1", :eu, :pending})
      assert [["o-1"]] == bindings
    end

    test "scans all partitions with :_ wildcard" do
      Cache.put!({:order, "o-10", :eu, :pending})
      Cache.put!({:order, "o-11", :us, :pending})
      ids = Cache.get_by_match!(:_, {:order, :"$1", :_, :pending}) |> Enum.sort()
      assert [["o-10"], ["o-11"]] == ids
    end

    test "single-arg form defaults to :_ (all partitions)" do
      Cache.put!({:order, "o-20", :eu, :pending})
      ids = Cache.get_by_match!({:order, :"$1", :eu, :pending})
      assert [["o-20"]] == ids
    end

    test "returns empty list when no records match" do
      assert [] == Cache.get_by_match!(:_, {:ghost, :_, :_, :_})
    end

    test "primary read_mode returns same result" do
      Cache.put!({:order, "o-30", :eu, :pending})
      local = Cache.get_by_match!(:order, {:order, :"$1", :eu, :pending})
      primary = Cache.get_by_match!(:order, {:order, :"$1", :eu, :pending}, read_mode: :primary)
      assert local == primary
    end
  end

  describe "get_by_match/3 non-bang" do
    test "returns binding lists on success" do
      Cache.put!({:order, "o-nb", :eu, :pending})
      result = Cache.get_by_match(:order, {:order, :"$1", :eu, :pending})
      assert [["o-nb"]] == result
    end
  end

  # ── get_by_match_object! ──────────────────────────────────────────────────────

  describe "get_by_match_object!/3" do
    test "returns full tuples for a specific partition" do
      Cache.put!({:product, "p-1", :eu, "Widget", 9.99})
      Cache.put!({:product, "p-2", :eu, "Gadget", 24.99})
      # partition val = :product
      results =
        Cache.get_by_match_object!(:product, {:product, :_, :eu, :_, :_})
        |> Enum.sort()

      assert [
               {:product, "p-1", :eu, "Widget", 9.99},
               {:product, "p-2", :eu, "Gadget", 24.99}
             ] == results
    end

    test "scans all partitions with :_" do
      Cache.put!({:product, "p-a", :eu, "W1", 1.0})
      Cache.put!({:product, "p-b", :us, "W2", 2.0})

      results =
        Cache.get_by_match_object!(:_, {:product, :_, :_, :_, :_})
        |> Enum.sort_by(fn {:product, id, _, _, _} -> id end)

      assert length(results) >= 2
      assert Enum.any?(results, &match?({:product, "p-a", :eu, "W1", 1.0}, &1))
      assert Enum.any?(results, &match?({:product, "p-b", :us, "W2", 2.0}, &1))
    end

    test "single-arg form scans all partitions" do
      Cache.put!({:product, "p-sa", :eu, "Widget", 9.99})
      results = Cache.get_by_match_object!({:product, :_, :eu, :_, :_})
      assert Enum.any?(results, &match?({:product, "p-sa", :eu, "Widget", 9.99}, &1))
    end

    test "returns empty list when nothing matches" do
      assert [] == Cache.get_by_match_object!(:_, {:ghost, :_, :_})
    end

    test "quorum read returns same result as local" do
      Cache.put!({:product, "p-q", :eu, "QuorumW", 5.0})
      local = Cache.get_by_match_object!(:product, {:product, :_, :eu, :_, :_})

      quorum =
        Cache.get_by_match_object!(:product, {:product, :_, :eu, :_, :_}, read_mode: :quorum)

      assert Enum.sort(local) == Enum.sort(quorum)
    end
  end

  describe "get_by_match_object/3 non-bang" do
    test "returns full tuples on success" do
      Cache.put!({:product, "p-nb", :eu, "NB", 3.0})
      result = Cache.get_by_match_object(:product, {:product, :_, :eu, :_, :_})
      assert Enum.any?(result, &match?({:product, "p-nb", :eu, "NB", 3.0}, &1))
    end
  end

  # ── scan! ─────────────────────────────────────────────────────────────────────

  describe "scan!/3 and scan!/2" do
    test "folds over all records in a specific partition" do
      Cache.put!({:metric, "m1", :host1, 10})
      Cache.put!({:metric, "m2", :host1, 20})
      # partition_pos: 0 → partition val = :metric
      total = Cache.scan!(:metric, fn {_, _, _, v}, acc -> acc + v end, 0)
      assert total >= 30
    end

    test "two-arg form folds over all partitions" do
      Cache.put!({:metric, "ma", :h1, 5})
      Cache.put!({:counter, "cb", :h2, 3})
      count = Cache.scan!(fn _rec, acc -> acc + 1 end, 0)
      assert count >= 2
    end

    test "returns the accumulator unchanged on empty cache" do
      Cache.delete_all()
      assert 0 == Cache.scan!(fn _rec, acc -> acc + 1 end, 0)
    end

    test "non-bang scan/3 returns result" do
      Cache.put!({:metric, "ms", :h, 7})
      result = Cache.scan(:metric, fn {_, _, _, v}, acc -> acc + v end, 0)
      assert result >= 7
    end
  end

  # ── delete! ───────────────────────────────────────────────────────────────────

  describe "delete!/1" do
    test "removes an existing record" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.delete!({:user, 1, nil, nil})
      assert [] == Cache.get!({:user, 1, nil, nil})
    end

    test "delete! on missing key is a no-op" do
      assert :ok == Cache.delete!({:user, 9999, nil, nil})
    end

    test "only removes the targeted record" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.put!({:user, 2, "Bob", :member})
      Cache.delete!({:user, 1, nil, nil})
      assert [] == Cache.get!({:user, 1, nil, nil})
      assert [{:user, 2, "Bob", :member}] == Cache.get!({:user, 2, nil, nil})
    end

    test "record is gone when read via :primary after delete" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.delete!({:user, 1, nil, nil})
      assert [] == Cache.get!({:user, 1, nil, nil}, read_mode: :primary)
    end
  end

  describe "delete/1 non-bang" do
    test "returns :ok on success" do
      Cache.put!({:user, 77, "Z", :member})
      assert :ok == Cache.delete({:user, 77, nil, nil})
      assert [] == Cache.get!({:user, 77, nil, nil})
    end

    test "returns {:error, _} on bad input" do
      assert {:error, _} = Cache.delete(:not_a_tuple)
    end
  end

  # ── delete_all ────────────────────────────────────────────────────────────────

  describe "delete_all/0" do
    test "removes all records across all partitions" do
      for i <- 1..10, do: Cache.put!({:user, i, "P#{i}", :member})
      Cache.delete_all()

      for i <- 1..10 do
        assert [] == Cache.get!({:user, i, nil, nil})
      end
    end

    test "delete_all on empty cache is a no-op" do
      assert :ok == Cache.delete_all()
    end

    test "cache is usable after delete_all" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.delete_all()
      Cache.put!({:user, 2, "Bob", :member})
      assert [] == Cache.get!({:user, 1, nil, nil})
      assert [{:user, 2, "Bob", :member}] == Cache.get!({:user, 2, nil, nil})
    end
  end

  # ── delete_by_match! ──────────────────────────────────────────────────────────

  describe "delete_by_match!/2" do
    test "removes all records matching a pattern in a specific partition" do
      Cache.put!({:session, "tok-1", :eu, :active})
      Cache.put!({:session, "tok-2", :eu, :expired})
      Cache.put!({:session, "tok-3", :us, :active})
      # partition_pos: 0 → partition key is elem(tuple, 0) = :session for all
      # session records, so we pass :session (not :eu) as partition_data.
      Cache.delete_by_match!(:session, {:session, :_, :eu, :expired})
      assert [] == Cache.get!({:session, "tok-2", nil, nil})
      assert [{:session, "tok-1", :eu, :active}] == Cache.get!({:session, "tok-1", nil, nil})
      assert [{:session, "tok-3", :us, :active}] == Cache.get!({:session, "tok-3", nil, nil})
    end

    test "wildcard :_ removes matching records across all partitions" do
      Cache.put!({:tmp_lock, "a", :eu, nil})
      Cache.put!({:tmp_lock, "b", :us, nil})
      Cache.put!({:user, 1, :eu, :admin})
      Cache.delete_by_match!(:_, {:tmp_lock, :_, :_, :_})
      assert [] == Cache.get!({:tmp_lock, "a", nil, nil})
      assert [] == Cache.get!({:tmp_lock, "b", nil, nil})
      # Non-matching records survive.
      assert [{:user, 1, :eu, :admin}] == Cache.get!({:user, 1, nil, nil})
    end

    test "no-op when pattern matches nothing" do
      Cache.put!({:user, 1, "Alice", :admin})
      Cache.delete_by_match!(:_, {:ghost, :_, :_, :_})
      assert [{:user, 1, "Alice", :admin}] == Cache.get!({:user, 1, nil, nil})
    end

    test "single-arg bang form deletes from all partitions" do
      Cache.put!({:tmp_lock, "x", :eu, nil})
      Cache.delete_by_match!({:tmp_lock, :_, :_, :_})
      assert [] == Cache.get!({:tmp_lock, "x", nil, nil})
    end
  end

  describe "delete_by_match/2 non-bang" do
    test "returns :ok on success" do
      Cache.put!({:session, "nb-tok", :eu, :expired})
      assert :ok == Cache.delete_by_match(:session, {:session, :_, :eu, :expired})
      assert [] == Cache.get!({:session, "nb-tok", nil, nil})
    end
  end

  # ── delete_by_key_partition! ──────────────────────────────────────────────────

  describe "delete_by_key_partition!/2" do
    test "removes a record by explicit key and partition" do
      Cache.put!({:user, 42, "Dave", :member})
      # key = 42 (key_pos: 1), partition determined by :user (partition_pos: 0)
      Cache.delete_by_key_partition!(42, :user)
      assert [] == Cache.get!({:user, 42, nil, nil})
    end

    test "no-op when key does not exist in partition" do
      assert :ok == Cache.delete_by_key_partition!(9999, :eu)
    end

    test "only removes the target key, not siblings" do
      Cache.put!({:user, 100, "A", :admin})
      Cache.put!({:user, 101, "B", :member})
      Cache.delete_by_key_partition!(100, :user)
      assert [] == Cache.get!({:user, 100, nil, nil})
      assert [{:user, 101, "B", :member}] == Cache.get!({:user, 101, nil, nil})
    end
  end

  describe "delete_by_key_partition/2 non-bang" do
    test "returns :ok on success" do
      Cache.put!({:user, 200, "NB", :member})
      assert :ok == Cache.delete_by_key_partition(200, :user)
      assert [] == Cache.get!({:user, 200, nil, nil})
    end
  end

  # ── delete_same_key_partition! ────────────────────────────────────────────────

  describe "delete_same_key_partition!/1" do
    test "deletes when partition_pos == key_pos is simulated" do
      # We can test this by using a value that hashes the same way for both.
      # In our setup key_pos=1, partition_pos=0, so we just verify it delegates
      # correctly and returns :ok (even if the key doesn't exist in that part).
      assert :ok == Cache.delete_same_key_partition!(:some_key)
    end
  end

  describe "delete_same_key_partition/1 non-bang" do
    test "returns :ok" do
      assert :ok == Cache.delete_same_key_partition(:some_key)
    end
  end

  # ── stats/0 and cluster_stats/0 ───────────────────────────────────────────────

  describe "stats/0" do
    test "returns a keyword list with per-partition counts and :total" do
      Cache.put!({:user, 1, "Alice", :admin})
      stats = Cache.stats()
      assert Keyword.keyword?(stats)
      assert Keyword.has_key?(stats, :total)
      assert Keyword.get(stats, :total) >= 1
    end

    test "total is 0 after delete_all" do
      Cache.delete_all()
      assert 0 == Cache.stats() |> Keyword.get(:total)
    end

    test "total increases after puts" do
      Cache.delete_all()
      for i <- 1..5, do: Cache.put!({:user, i, "U#{i}", :member})
      assert Cache.stats() |> Keyword.get(:total) >= 5
    end
  end

  describe "cluster_stats/0" do
    test "returns a map with expected top-level keys" do
      stats = Cache.cluster_stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :nodes)
      assert Map.has_key?(stats, :node_count)
      assert Map.has_key?(stats, :replication_mode)
      assert Map.has_key?(stats, :num_partitions)
      assert Map.has_key?(stats, :total_records)
      assert Map.has_key?(stats, :node_stats)
      assert Map.has_key?(stats, :unreachable_nodes)
    end

    test "unreachable_nodes is empty in a single-node setup" do
      assert [] == Cache.cluster_stats().unreachable_nodes
    end

    test "node_stats contains the local node" do
      stats = Cache.cluster_stats()
      assert Map.has_key?(stats.node_stats, node())
    end

    test "total_records reflects stored data" do
      Cache.delete_all()
      for i <- 1..3, do: Cache.put!({:user, i, "U#{i}", :member})
      assert Cache.cluster_stats().total_records >= 3
    end
  end

  # ── replication_mode: :strong (3PC) ──────────────────────────────────────────

  describe "put!/1 under :strong replication_mode" do
    test "write and read survive 3PC round-trip" do
      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 1,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :strong,
        num_partition: 4
      )

      Cache.put!({:user, 1, "Alice", :admin})

      assert [{:user, 1, "Alice", :admin}] ==
               Cache.get!({:user, 1, nil, nil}, read_mode: :primary)

      Cache.delete_all()

      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 1,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :async,
        num_partition: 4
      )
    end

    test "delete! under :strong removes the record" do
      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 1,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :strong,
        num_partition: 4
      )

      Cache.put!({:user, 5, "Strong", :admin})
      Cache.delete!({:user, 5, nil, nil})
      assert [] == Cache.get!({:user, 5, nil, nil})

      SuperCache.stop()
      Process.sleep(50)

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 1,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 2,
        replication_mode: :async,
        num_partition: 4
      )
    end
  end

  # ── started?/0 ────────────────────────────────────────────────────────────────

  describe "started?/0" do
    test "returns true when cluster bootstrap has completed" do
      assert true == Cache.started?()
    end
  end

  # ── error handling (non-bang) ─────────────────────────────────────────────────

  describe "non-bang error handling" do
    test "put/1 returns {:error, _} on bad input" do
      assert {:error, _} = Cache.put(:not_a_tuple)
    end

    test "get/2 returns {:error, _} on bad input" do
      assert {:error, _} = Cache.get(:not_a_tuple)
    end

    test "delete/1 returns {:error, _} on bad input" do
      assert {:error, _} = Cache.delete(:not_a_tuple)
    end
  end

  # ── complex ───────────────────────────────────────────────────────────────────

  describe "complex scenarios" do
    test "high-volume put and get across multiple partitions" do
      records = for i <- 1..50, do: {:item, i, :eu, i * 10}
      Enum.each(records, &Cache.put!/1)

      Enum.each(records, fn {:item, i, _, _} = r ->
        assert [r] == Cache.get!({:item, i, nil, nil})
      end)
    end

    test "interleaved writes and deletes leave cache consistent" do
      for i <- 1..20, do: Cache.put!({:user, i, "User#{i}", :member})
      # Delete odd ids (1, 3, 5 … 19).  key_pos: 1 so key = i.
      for i <- 1..20//2, do: Cache.delete!({:user, i, nil, nil})

      for i <- 1..20//2, do: assert([] == Cache.get!({:user, i, nil, nil}))

      for i <- 2..20//2 do
        assert [{:user, i, "User#{i}", :member}] == Cache.get!({:user, i, nil, nil})
      end
    end

    test "all three read modes agree after a write" do
      Cache.put!({:user, 99, "Eve", :admin})

      local = Cache.get!({:user, 99, nil, nil}, read_mode: :local)
      primary = Cache.get!({:user, 99, nil, nil}, read_mode: :primary)
      quorum = Cache.get!({:user, 99, nil, nil}, read_mode: :quorum)

      assert [{:user, 99, "Eve", :admin}] == local
      assert local == primary
      assert local == quorum
    end

    test "delete_all resets state between logical sessions" do
      for i <- 1..10, do: Cache.put!({:user, i, "U#{i}", :member})
      Cache.delete_all()
      for i <- 1..5, do: Cache.put!({:user, i, "New#{i}", :admin})

      for i <- 1..5 do
        assert [{:user, i, "New#{i}", :admin}] == Cache.get!({:user, i, nil, nil})
      end

      for i <- 6..10 do
        assert [] == Cache.get!({:user, i, nil, nil})
      end
    end

    test "match_object and scan are consistent after writes" do
      records = for i <- 1..5, do: {:product, "p-#{i}", :eu, "W#{i}", i * 1.0}
      Enum.each(records, &Cache.put!/1)

      matched = Cache.get_by_match_object!(:product, {:product, :_, :eu, :_, :_}) |> Enum.sort()

      scanned =
        Cache.scan!(
          :product,
          fn
            {:product, _, :eu, _, _} = r, acc -> [r | acc]
            _, acc -> acc
          end,
          []
        )
        |> Enum.sort()

      assert matched == scanned
    end

    test "get_by_match returns only requested captures" do
      Cache.put!({:order, "ord-1", :eu, :pending, 100.0})
      Cache.put!({:order, "ord-2", :eu, :shipped, 200.0})
      Cache.put!({:order, "ord-3", :eu, :pending, 150.0})

      # Capture only the ids of pending orders in :order partition
      ids =
        Cache.get_by_match!(:order, {:order, :"$1", :eu, :pending, :_})
        |> Enum.sort()

      assert [["ord-1"], ["ord-3"]] == ids
    end

    test "delete_by_key_partition removes only the right record in crowded partition" do
      for i <- 1..10, do: Cache.put!({:user, i, "U#{i}", :member})
      Cache.delete_by_key_partition!(5, :user)

      assert [] == Cache.get!({:user, 5, nil, nil})

      for i <- [1, 2, 3, 4, 6, 7, 8, 9, 10] do
        assert [{:user, i, "U#{i}", :member}] == Cache.get!({:user, i, nil, nil})
      end
    end
  end
end
