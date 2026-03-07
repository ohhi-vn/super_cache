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
  end

  # ── error handling (non-bang) ─────────────────────────────────────────────────

  describe "non-bang error handling" do
    test "put/1 returns {:error, _} on bad input" do
      assert {:error, _} = Cache.put(:not_a_tuple)
    end

    test "get/2 returns {:error, _} on bad input" do
      assert {:error, _} = Cache.get(:not_a_tuple)
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
  end
end
