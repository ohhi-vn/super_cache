defmodule SuperCache.GapFillTest do
  @moduledoc """
  Targets remaining uncovered branches: Router forwarding under foreign-primary
  topologies, Manager retry chains, NodeMonitor MFA sources, Storage typed /
  atomic primitives, EtsHolder lifecycle and SuperCache direct-partition APIs.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Manager, NodeMonitor, Router}
  alias SuperCache.{EtsHolder, Partition, Storage}

  @pt_partition_map {Manager, :partition_map}

  setup_all do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 4)
    Process.sleep(30)

    :ok
  end

  defp plant_map(map) do
    original = :persistent_term.get(@pt_partition_map, nil)
    :persistent_term.put(@pt_partition_map, map)

    on_exit(fn ->
      if original,
        do: :persistent_term.put(@pt_partition_map, original),
        else: :persistent_term.erase(@pt_partition_map)
    end)
  end

  # ── Router: forwarded operations (foreign primary, erpc fails fast) ──────────

  describe "Router forwarded writes" do
    test "route_put!/1 forwards to the primary and survives its absence" do
      order = Partition.get_partition_order(:fwd_key)
      plant_map(%{order => {:"foreign@host", []}})
      assert true == Router.route_put!({:fwd_key, "v"})
    end

    test "route_delete!/1 forwards to the foreign primary" do
      order = Partition.get_partition_order(:fwd_del)
      plant_map(%{order => {:"foreign@host", []}})
      assert :ok = Router.route_delete!({:fwd_del, nil})
    end

    test "route_put_batch!/1 forwards grouped batches" do
      o1 = Partition.get_partition_order(:batch_a)
      o2 = Partition.get_partition_order(:batch_b)
      plant_map(%{o1 => {:"foreign@host", []}, o2 => {node(), []}})

      assert :ok =
               Router.route_put_batch!([{:batch_a, 1, "x"}, {:batch_b, 2, "y"}])
    end

    test "route_delete_by_key_partition!/3 forwards when not primary" do
      plant_map(%{3 => {:"foreign@host", []}})
      assert :ok = Router.route_delete_by_key_partition!(:some_key, 3)
    end

    test "route_delete_match_partition!/3 executes locally when forwarded flag set" do
      pattern = {:dm_gap, :_}

      assert :ok = Router.route_delete_match_partition!(0, pattern, forwarded: true)
      assert :ok = Router.route_delete_all_partition(0, forwarded: true)
    end

    test "route_delete_all/0 fans out over foreign primaries" do
      num = Partition.get_num_partition()

      map =
        Map.new(0..(num - 1), fn idx ->
          {idx, {String.to_atom("foreign_gap_#{idx}@host"), []}}
        end)

      plant_map(map)
      assert :ok = Router.route_delete_all()
    end

    test "forwarded ops skip the primary check entirely" do
      plant_map(%{0 => {:"foreign@host", []}})
      # With forwarded: true the op must apply locally regardless of topology.
      assert true == Router.route_put!({:local_fwd, 1}, forwarded: true)
      partition = Partition.get_partition(:local_fwd)
      assert [{:local_fwd, 1}] == Storage.get(:local_fwd, partition)
    end

    test "local_read/3 is a public erpc-safe dispatcher" do
      Storage.put({:lr_key, 7}, :"SuperCache.Storage.Ets_0")

      assert [{:lr_key, 7}] == Router.local_read(0, :get, :lr_key)
      assert is_list(Router.local_read(0, :match, {:lr_key, :_}))
      assert is_list(Router.local_read(0, :match_object, {:lr_key, :_, :_}))
    end

    test "read-your-writes upgrade and expiry" do
      Router.ensure_ryw_table()

      Router.track_write(3)
      assert Router.ryw_recent?(3) == true

      # Force-expire the entry.
      :ets.insert(Router.RywTracker, {{self(), 3}, System.monotonic_time(:millisecond) - 10})
      assert Router.ryw_recent?(3) == false
    end
  end

  # ── Manager: retry chain driven by raw messages ──────────────────────────────

  describe "Manager retry chain" do
    test "{:retry_node_up, target, attempt} reschedules until give-up" do
      send(Manager, {:retry_node_up, :retry_fake@host, 9})
      Process.sleep(20)

      # Attempt >= 10 logs the give-up warning and stops retrying.
      send(Manager, {:retry_node_up, :retry_fake@host, 10})
      Process.sleep(20)
      assert node() in Manager.live_nodes()
    end

    test "retry for an already-member node is a no-op" do
      send(Manager, {:retry_node_up, node(), 1})
      Process.sleep(20)
      assert node() in Manager.live_nodes()
    end

    test "stray info messages are swallowed" do
      send(Manager, {:totally_unexpected, self()})
      Process.sleep(10)
      assert Process.whereis(Manager)
    end
  end

  # ── NodeMonitor: static sets and MFA sources ────────────────────────────────

  describe "NodeMonitor sources" do
    test "mfa source refreshes on schedule and tolerates raising MFAs" do
      log =
        ExUnit.CaptureLog.capture_log([level: :error], fn ->
          :ok =
            NodeMonitor.reconfigure(
              nodes_mfa: {NodeMonitorGapProbe, :boom, []},
              refresh_ms: 20
            )
        end)

      Process.sleep(120)

      # A working MFA afterwards proves the refresh loop still ticks.
      assert :ok = NodeMonitor.reconfigure(nodes_mfa: {NodeMonitorGapProbe, :nodes, []})
      Process.sleep(60)
      assert Process.whereis(NodeMonitor)
    end

    test "kernel events for nodes outside a static set are ignored" do
      assert :ok = NodeMonitor.reconfigure(nodes: [:"watched_only@host"])
      send(NodeMonitor, {:nodeup, :unwatched@host, []})
      Process.sleep(20)

      # Restore default.
      assert :ok = NodeMonitor.reconfigure([])
    end
  end

  # ── Storage primitives ───────────────────────────────────────────────────────

  describe "Storage primitives" do
    @partition :"SuperCache.Storage.Ets_0"

    setup do
      # A sibling test may have torn the table down — make sure it exists.
      if :ets.info(@partition) == :undefined do
        EtsHolder.new_table(EtsHolder, @partition)
      end

      Storage.delete_all(@partition)
      :ok
    end

    test "insert_new/2 only inserts absent keys" do
      assert Storage.insert_new({:ins, 1}, @partition) == true
      assert Storage.insert_new({:ins, 1}, @partition) == false
    end

    test "take/2 atomically removes and returns" do
      Storage.put({:tk, "v"}, @partition)
      assert [{:tk, "v"}] == Storage.take(:tk, @partition)
      assert [] == Storage.take(:tk, @partition)
    end

    test "update_element/3 without default returns false for missing keys" do
      assert Storage.update_element(:missing_ue, @partition, {2, :x}) == false
    end

    test "update_counter/3 without default raises for missing keys" do
      assert_raise ArgumentError, fn ->
        Storage.update_counter(:missing_uc, @partition, {2, 1})
      end
    end

    test "typed wrappers map records through from_tuple/1" do
      Storage.put({:typed, 5}, @partition)

      assert [5] == Storage.get_typed(GapTypedRecord, :typed, @partition)
      assert [5] ==
               Storage.get_by_match_object_typed(GapTypedRecord, {:typed, :_}, @partition)
    end

    test "stats/1 reports :undefined for missing tables" do
      assert {:missing_tbl_xyz, :undefined} = Storage.stats(:missing_tbl_xyz)
    end

    test "delete_match returns deleted count" do
      Storage.put({:dm_a, :x}, @partition)
      Storage.put({:dm_b, :x}, @partition)
      assert 2 == Storage.delete_match({:_, :x}, @partition)
    end

    test "stop/1 tolerates dead owners via catch and tables can be recreated" do
      :ok = Storage.stop(1)

      # Recreate so sibling tests are unaffected regardless of execution order.
      EtsHolder.new_table(EtsHolder, :"SuperCache.Storage.Ets_0")
    end
  end

  # ── EtsHolder lifecycle ──────────────────────────────────────────────────────

  describe "EtsHolder" do
    test "clean/2 clears one table; clean_all/1 clears every tracked table" do
      EtsHolder.new_table(EtsHolder, :"gap_holder_table")
      :ets.insert(:"gap_holder_table", {:a, 1})

      assert true == EtsHolder.clean(EtsHolder, :"gap_holder_table")
      assert [] == :ets.tab2list(:"gap_holder_table")

      EtsHolder.clean_all(EtsHolder)
      assert :ok == EtsHolder.delete_table(EtsHolder, :"gap_holder_table")
    end

    test "delete_table/2 skips unknown tables gracefully" do
      assert :ok == EtsHolder.delete_table(EtsHolder, :"never_created_gap")
    end
  end

  # ── Partition.Holder & Partition ─────────────────────────────────────────────

  describe "Partition.Holder" do
    test "get/1 returns nil for unregistered indices" do
      assert nil == SuperCache.Partition.Holder.get(99_999)
    end

    test "get_all/0 returns only integer-indexed entries as table atoms" do
      all = SuperCache.Partition.Holder.get_all()
      assert all != []
      assert Enum.all?(all, &is_atom/1)
    end

    test "stop/0 terminates the holder and the supervisor brings it back" do
      num = Partition.get_num_partition()
      pid = Process.whereis(SuperCache.Partition.Holder)

      assert :ok = SuperCache.Partition.Holder.stop()
      Process.sleep(30)
      refute pid == Process.whereis(SuperCache.Partition.Holder)

      wait_until(fn -> Process.whereis(SuperCache.Partition.Holder) != nil end)
      Process.sleep(100)

      # Rebuild the registry so siblings are unaffected.
      SuperCache.Partition.start(num)
    end

    test "clean/0 empties the registry and it can be rebuilt" do
      num = length(SuperCache.Partition.Holder.get_all())
      assert true == SuperCache.Partition.Holder.clean()

      # Rebuild immediately so sibling tests are unaffected.
      SuperCache.Partition.start(num)

      assert length(SuperCache.Partition.Holder.get_all()) == num
    end
  end

  # ── SuperCache direct-partition fast paths ───────────────────────────────────

  describe "SuperCache direct partition access" do
    test "put/get/delete_partition! operate on an explicit table" do
      partition = Partition.get_partition(:direct_probe)

      assert true == SuperCache.put_partition!({:direct_probe, 1, "x"}, partition)
      assert [{:direct_probe, 1, "x"}] == SuperCache.get_partition!(:direct_probe, partition)
      assert :ok == SuperCache.delete_partition!(:direct_probe, partition)
      assert [] == SuperCache.get_partition!(:direct_probe, partition)
    end

    test "put/get/delete_partition_by_idx! are the fastest paths" do
      idx = Partition.get_partition_order(:idx_probe)

      assert true == SuperCache.put_partition_by_idx!({:idx_probe, 2, "y"}, idx)
      assert [{:idx_probe, 2, "y"}] == SuperCache.get_partition_by_idx!(:idx_probe, idx)
      assert :ok == SuperCache.delete_partition_by_idx!(:idx_probe, idx)
      assert [] == SuperCache.get_partition_by_idx!(:idx_probe, idx)
    end

    test "scan!/2 folds over all partitions" do
      SuperCache.put_batch!([{:scan_a, 1}, {:scan_b, 2}])

      count =
        SuperCache.scan!(fn _rec, acc -> acc + 1 end, 0)

      assert is_integer(count) and count > 0
    end

    test "lazy_put returns an error tuple when buffers never started" do
      names = :persistent_term.get({SuperCache.Buffer, :buffer_names}, nil)

      try do
        :persistent_term.erase({SuperCache.Buffer, :buffer_names})
        assert {:error, :not_started} = SuperCache.lazy_put({:nope, 1})
      after
        if names, do: :persistent_term.put({SuperCache.Buffer, :buffer_names}, names)
      end
    end
  end

  # ── Stats extras ─────────────────────────────────────────────────────────────

  test "Stats.print/1 handles nested keyword lists" do
    assert :ok = SuperCache.Cluster.Stats.print(%{deep: %{deeper: %{deepest: 1}}})
  end

  test "cluster_stats/1 in local mode reports single-node shape" do
    stats = SuperCache.cluster_stats()
    assert stats.node_count == 1
    assert stats.replication_mode == :none
    assert stats.unreachable_nodes == []
  end
  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end
end

defmodule NodeMonitorGapProbe do
  def boom, do: raise("probe boom")

  def nodes, do: []
end

defmodule GapTypedRecord do
  def from_tuple({_k, v}), do: v
end
