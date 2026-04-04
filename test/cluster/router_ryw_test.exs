defmodule SuperCache.Cluster.RouterRywTest do
  @moduledoc """
  Tests for read-your-writes consistency in the Router.

  Verifies that:
  - After a write, subsequent reads of the same partition are auto-routed to primary
  - RYW tracking entries expire after the TTL window
  - The prune_ryw function correctly removes expired entries
  - The RYW table is created lazily on first use
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.Router
  alias SuperCache.{Config, Partition}

  @ryw_table Router.RywTracker

  setup do
    # Ensure the RYW table exists for testing
    Router.ensure_ryw_table()

    on_exit(fn ->
      try do
        :ets.delete(@ryw_table)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  describe "read-your-writes tracking" do
    test "track_write records the partition order with expiry" do
      order = 3
      Router.track_write(order)

      now = System.monotonic_time(:millisecond)
      ttl = 5_000

      [{key, expiry}] = :ets.lookup(@ryw_table, {self(), order})
      assert key == {self(), order}
      assert expiry > now
      assert expiry <= now + ttl
    end

    test "ryw_recent? returns true for recent writes" do
      order = 5
      Router.track_write(order)

      assert Router.ryw_recent?(order) == true
    end

    test "ryw_recent? returns false for unwritten partitions" do
      # Use an order that was never written to
      assert Router.ryw_recent?(999) == false
    end

    test "ryw_recent? returns false after TTL expires" do
      order = 7
      Router.track_write(order)

      # Manually set the expiry to the past
      past_expiry = System.monotonic_time(:millisecond) - 1_000
      :ets.insert(@ryw_table, {{self(), order}, past_expiry})

      assert Router.ryw_recent?(order) == false

      # Entry should be cleaned up
      assert :ets.lookup(@ryw_table, {self(), order}) == []
    end
  end

  describe "resolve_read_mode" do
    test "upgrades :local to :primary for recently written partition" do
      order = 2
      Router.track_write(order)

      assert Router.resolve_read_mode(:local, order) == :primary
    end

    test "keeps :local for partitions not recently written" do
      assert Router.resolve_read_mode(:local, 999) == :local
    end

    test "does not change :primary mode" do
      assert Router.resolve_read_mode(:primary, 999) == :primary
    end

    test "does not change :quorum mode" do
      assert Router.resolve_read_mode(:quorum, 999) == :quorum
    end
  end

  describe "prune_ryw" do
    test "removes expired entries for the calling process" do
      # Write to multiple partitions
      Enum.each([1, 2, 3], fn order ->
        Router.track_write(order)
      end)

      # Set all entries to expired
      now = System.monotonic_time(:millisecond)
      Enum.each([1, 2, 3], fn order ->
        :ets.insert(@ryw_table, {{self(), order}, now - 1_000})
      end)

      # Prune
      Router.prune_ryw(now)

      # All entries should be gone
      Enum.each([1, 2, 3], fn order ->
        assert :ets.lookup(@ryw_table, {self(), order}) == []
      end)
    end

    test "does not remove non-expired entries" do
      order = 4
      future_expiry = System.monotonic_time(:millisecond) + 60_000
      :ets.insert(@ryw_table, {{self(), order}, future_expiry})

      Router.prune_ryw(System.monotonic_time(:millisecond))

      [{key, expiry}] = :ets.lookup(@ryw_table, {self(), order})
      assert key == {self(), order}
      assert expiry == future_expiry
    end

    test "does not remove entries for other processes" do
      order = 5
      future_expiry = System.monotonic_time(:millisecond) + 60_000

      # Insert entry for a fake PID
      fake_pid = :erlang.list_to_pid(~c"<0.999.0>")
      :ets.insert(@ryw_table, {{fake_pid, order}, future_expiry})

      Router.prune_ryw(System.monotonic_time(:millisecond))

      # Entry for other process should still exist
      [{key, expiry}] = :ets.lookup(@ryw_table, {fake_pid, order})
      assert key == {fake_pid, order}
      assert expiry == future_expiry
    end
  end

  describe "ensure_ryw_table" do
    test "creates the table if it does not exist" do
      # Delete the table
      :ets.delete(@ryw_table)

      # Ensure it is recreated
      Router.ensure_ryw_table()

      assert :ets.info(@ryw_table) != :undefined
      assert :ets.info(@ryw_table, :name) == @ryw_table
    end

    test "is idempotent when table already exists" do
      Router.ensure_ryw_table()
      Router.ensure_ryw_table()

      assert :ets.info(@ryw_table) != :undefined
    end
  end
end
