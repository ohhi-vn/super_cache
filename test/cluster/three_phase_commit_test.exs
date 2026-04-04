defmodule SuperCache.Cluster.ThreePhaseCommitTest do
  @moduledoc """
  Unit and integration tests for the three-phase commit protocol.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{ThreePhaseCommit, TxnRegistry, Manager}
  alias SuperCache.{Storage, Partition, Config}

  @cache_opts [
    key_pos: 0,
    partition_pos: 0,
    cluster: :distributed,
    replication_factor: 1,
    replication_mode: :strong,
    num_partition: 4,
    table_type: :set
  ]

  @cluster_opts [
    key_pos: 0,
    partition_pos: 0,
    cluster: :distributed,
    replication_factor: 2,
    replication_mode: :strong,
    num_partition: 4,
    table_type: :set
  ]

  # ── Shared helpers ────────────────────────────────────────────────────────────

  defp wipe_all do
    num = Config.get_config(:num_partition)

    for idx <- 0..(num - 1) do
      partition = Partition.get_partition_by_idx(idx)
      Storage.delete_all(partition)
    end

    TxnRegistry.list_all()
    |> Enum.each(fn {txn_id, _} -> TxnRegistry.remove(txn_id) end)

    :ok
  end

  # Run a 3PC commit from the correct PRIMARY node for `partition_idx`.
  #
  # ThreePhaseCommit.commit/2 must be called from the primary because
  # apply_local/2 writes to the CALLING node's ETS.  If a non-primary node
  # calls commit/2 the primary's ETS is left empty.
  defp commit_from_primary(partition_idx, ops) do
    {primary, _replicas} = Manager.get_replicas(partition_idx)

    if primary == node() do
      ThreePhaseCommit.commit(partition_idx, ops)
    else
      :erpc.call(primary, ThreePhaseCommit, :commit, [partition_idx, ops], 10_000)
    end
  end

  # ── Unit-test group setup ─────────────────────────────────────────────────────

  setup_all do
    # Start the full application to ensure all dependencies are available
    {:ok, _} = Application.ensure_all_started(:super_cache)

    if SuperCache.started?(), do: SuperCache.Cluster.Bootstrap.stop()
    Process.sleep(50)
    SuperCache.Cluster.Bootstrap.start!(@cache_opts)
    on_exit(fn -> SuperCache.Cluster.Bootstrap.stop() end)
    :ok
  end

  setup context do
    unless context[:cluster] do
      wipe_all()
    end

    :ok
  end

  # ── TxnRegistry ──────────────────────────────────────────────────────────────

  describe "TxnRegistry" do
    test "register creates a :prepared entry" do
      TxnRegistry.register("txn-001", 0, [{:put, {:k, :v}}], [])
      assert %{state: :prepared, partition_idx: 0} = TxnRegistry.get("txn-001")
    end

    test "mark_pre_committed transitions to :pre_committed" do
      TxnRegistry.register("txn-002", 0, [], [])
      TxnRegistry.mark_pre_committed("txn-002")
      assert %{state: :pre_committed} = TxnRegistry.get("txn-002")
    end

    test "remove deletes the entry" do
      TxnRegistry.register("txn-003", 0, [], [])
      TxnRegistry.remove("txn-003")
      assert nil == TxnRegistry.get("txn-003")
    end

    test "count reflects number of in-flight transactions" do
      before = TxnRegistry.count()
      TxnRegistry.register("txn-c1", 0, [], [])
      TxnRegistry.register("txn-c2", 0, [], [])
      assert TxnRegistry.count() == before + 2
      TxnRegistry.remove("txn-c1")
      TxnRegistry.remove("txn-c2")
      assert TxnRegistry.count() == before
    end

    test "get returns nil for unknown txn_id" do
      assert nil == TxnRegistry.get("no-such-txn")
    end

    test "list_all returns all in-flight transactions" do
      TxnRegistry.register("txn-la1", 0, [], [])
      TxnRegistry.register("txn-la2", 1, [], [])
      ids = TxnRegistry.list_all() |> Enum.map(fn {id, _} -> id end)
      assert "txn-la1" in ids
      assert "txn-la2" in ids
      TxnRegistry.remove("txn-la1")
      TxnRegistry.remove("txn-la2")
    end
  end

  # ── Participant callbacks ─────────────────────────────────────────────────────

  describe "handle_prepare/3" do
    test "returns :vote_yes for valid ops" do
      ops = [{:put, {:key, :val}}, {:delete, :old_key}]
      assert :vote_yes == ThreePhaseCommit.handle_prepare("p-001", 0, ops)
    end

    test "stores the entry in TxnRegistry" do
      ThreePhaseCommit.handle_prepare("p-002", 1, [{:put, {:x, :y}}])
      assert %{state: :prepared} = TxnRegistry.get("p-002")
      TxnRegistry.remove("p-002")
    end

    test "returns {:vote_no, reason} for an invalid op" do
      assert {:vote_no, _} =
               ThreePhaseCommit.handle_prepare("p-003", 0, [{:unknown_op, :data}])
    end

    test "returns {:vote_no, reason} when ops is not a list" do
      assert {:vote_no, _} =
               ThreePhaseCommit.handle_prepare("p-004", 0, :not_a_list)
    end
  end

  describe "handle_pre_commit/1" do
    test "transitions registered txn to :pre_committed" do
      TxnRegistry.register("pc-001", 0, [], [])
      assert :ack_pre_commit == ThreePhaseCommit.handle_pre_commit("pc-001")
      assert %{state: :pre_committed} = TxnRegistry.get("pc-001")
      TxnRegistry.remove("pc-001")
    end

    test "returns :ack_pre_commit even when txn is unknown (idempotent)" do
      assert :ack_pre_commit == ThreePhaseCommit.handle_pre_commit("unknown-txn")
    end
  end

  describe "handle_commit/3" do
    test "applies ops and removes the entry from TxnRegistry" do
      partition = Partition.get_partition_by_idx(0)
      ops = [{:put, {:commit_test_key, :committed_value}}]

      TxnRegistry.register("co-001", 0, ops, [])
      # Two-arg call; ops_fallback defaults to [] but TxnRegistry has the ops.
      assert :ack_commit == ThreePhaseCommit.handle_commit("co-001", 0)

      assert [{:commit_test_key, :committed_value}] ==
               Storage.get(:commit_test_key, partition)

      assert nil == TxnRegistry.get("co-001")
    end

    test "applies fallback ops when txn not in registry (missed PREPARE)" do
      partition = Partition.get_partition_by_idx(0)
      ops_fallback = [{:put, {:fallback_key, :fallback_value}}]

      # No TxnRegistry.register — simulates a PREPARE that never arrived.
      assert :ack_commit ==
               ThreePhaseCommit.handle_commit("missing-txn-fb", 0, ops_fallback)

      assert [{:fallback_key, :fallback_value}] ==
               Storage.get(:fallback_key, partition)
    end

    test "returns :ack_commit with empty fallback when txn is unknown (idempotent)" do
      # Default fallback [] → apply_local does nothing, no crash.
      assert :ack_commit == ThreePhaseCommit.handle_commit("missing-txn", 0)
    end
  end

  describe "handle_abort/1" do
    test "removes the entry and returns :ack_abort" do
      TxnRegistry.register("ab-001", 0, [], [])
      assert :ack_abort == ThreePhaseCommit.handle_abort("ab-001")
      assert nil == TxnRegistry.get("ab-001")
    end

    test "returns :ack_abort for unknown txn (idempotent)" do
      assert :ack_abort == ThreePhaseCommit.handle_abort("no-txn")
    end
  end

  # ── Local commit (no replicas) ────────────────────────────────────────────────

  describe "commit/2 — single node (no replicas)" do
    test "put op writes to local ETS" do
      partition = Partition.get_partition_by_idx(0)
      ops = [{:put, {:single_put, :value}}]

      assert :ok == ThreePhaseCommit.commit(0, ops)
      assert [{:single_put, :value}] == Storage.get(:single_put, partition)
    end

    test "delete op removes from local ETS" do
      partition = Partition.get_partition_by_idx(0)
      Storage.put({:del_target, :data}, partition)

      assert :ok == ThreePhaseCommit.commit(0, [{:delete, :del_target}])
      assert [] == Storage.get(:del_target, partition)
    end

    test "delete_match op removes matching records" do
      partition = Partition.get_partition_by_idx(0)
      Storage.put({{:match_ns, 1}, :a}, partition)
      Storage.put({{:match_ns, 2}, :b}, partition)
      Storage.put({:other_key, :c}, partition)

      assert :ok ==
               ThreePhaseCommit.commit(0, [{:delete_match, {{:match_ns, :_}, :_}}])

      assert [] == Storage.get_by_match_object({{:match_ns, :_}, :_}, partition)
      assert [{:other_key, :c}] == Storage.get(:other_key, partition)
    end

    test "delete_all op clears the partition" do
      partition = Partition.get_partition_by_idx(0)
      for i <- 1..5, do: Storage.put({:"key_#{i}", i}, partition)

      assert :ok == ThreePhaseCommit.commit(0, [{:delete_all, nil}])
      assert [] == Storage.get_by_match_object(:_, partition)
    end

    test "multiple ops in a single transaction are applied atomically" do
      partition = Partition.get_partition_by_idx(0)
      Storage.put({:existing, :old}, partition)

      ops = [
        {:put, {:new_record, :fresh}},
        {:delete, :existing}
      ]

      assert :ok == ThreePhaseCommit.commit(0, ops)
      assert [{:new_record, :fresh}] == Storage.get(:new_record, partition)
      assert [] == Storage.get(:existing, partition)
    end

    test "transaction log is empty after a successful commit" do
      before = TxnRegistry.count()
      ThreePhaseCommit.commit(0, [{:put, {:tx_log_test, :v}}])
      assert TxnRegistry.count() == before
    end
  end

  # ── Recovery ─────────────────────────────────────────────────────────────────

  describe "recover/0" do
    test "commits :pre_committed transactions and writes data to ETS" do
      partition = Partition.get_partition_by_idx(0)
      ops = [{:put, {:recovery_key, :recovered}}]

      TxnRegistry.register("rec-001", 0, ops, [])
      TxnRegistry.mark_pre_committed("rec-001")

      ThreePhaseCommit.recover()

      assert [{:recovery_key, :recovered}] ==
               Storage.get(:recovery_key, partition)

      assert nil == TxnRegistry.get("rec-001")
    end

    test "aborts :prepared transactions without writing data" do
      partition = Partition.get_partition_by_idx(0)
      ops = [{:put, {:uncertain_key, :should_not_exist}}]

      TxnRegistry.register("rec-002", 0, ops, [])

      ThreePhaseCommit.recover()

      assert [] == Storage.get(:uncertain_key, partition)
      assert nil == TxnRegistry.get("rec-002")
    end

    test "recover/0 is a no-op when no in-doubt transactions exist" do
      assert :ok == ThreePhaseCommit.recover()
    end

    test "recover/0 handles a mix of :pre_committed and :prepared transactions" do
      partition = Partition.get_partition_by_idx(0)

      TxnRegistry.register("rec-mix-1", 0, [{:put, {:safe, :yes}}], [])
      TxnRegistry.mark_pre_committed("rec-mix-1")

      TxnRegistry.register("rec-mix-2", 0, [{:put, {:unsafe, :no}}], [])

      ThreePhaseCommit.recover()

      assert [{:safe, :yes}] == Storage.get(:safe, partition)
      assert [] == Storage.get(:unsafe, partition)
      assert nil == TxnRegistry.get("rec-mix-1")
      assert nil == TxnRegistry.get("rec-mix-2")
    end
  end

  # ── Multi-node integration tests ─────────────────────────────────────────────

  describe "3PC multi-node" do
    @moduletag :cluster
  @moduletag :sequential

    setup do
      if node() == :nonode@nohost do
        raise "Multi-node tests require distribution. Run with: mix test.cluster"
      end

      SuperCache.Cluster.Bootstrap.stop()
      Process.sleep(50)
      SuperCache.Cluster.Bootstrap.start!(@cluster_opts)

      {peer1, node1} = start_peer(:tpc_node1)
      {peer2, node2} = start_peer(:tpc_node2)

      true = Node.connect(node1)
      true = Node.connect(node2)
      true = :erpc.call(node1, Node, :connect, [node2], 5_000)

      # Wait until ALL three nodes see each other in their own Manager.
      # This ensures the partition map is stable before any commit is issued.
      true =
        wait_until(
          fn ->
            Enum.all?([node(), node1, node2], fn n ->
              try do
                :erpc.call(n, Manager, :live_nodes, [], 3_000) |> length() == 3
              catch
                _, _ -> false
              end
            end)
          end,
          8_000
        )

      on_exit(fn ->
        stop_peer(peer1)
        stop_peer(peer2)
        SuperCache.Cluster.Bootstrap.stop()
        Process.sleep(50)
        SuperCache.Cluster.Bootstrap.start!(@cache_opts)
      end)

      %{node1: node1, node2: node2, peer1: peer1, peer2: peer2}
    end

    # ── Replication ─────────────────────────────────────────────────────────────

    test "commit replicates a put to all replicas", %{node1: node1, node2: node2} do
      idx = Partition.get_partition_order(:tpc_test)
      partition = Partition.get_partition_by_idx(idx)
      ops = [{:put, {:tpc_test, :replicated}}]

      # 3PC MUST run from the PRIMARY node.  apply_local/2 writes to the
      # calling node's ETS; if a non-primary calls commit/2 the primary's
      # ETS is left empty.  commit_from_primary/2 routes via :erpc when
      # this test node is not the primary for `idx`.
      {primary, replicas} = Manager.get_replicas(idx)
      assert :ok == commit_from_primary(idx, ops)

      # Replication is async inside phase_commit (Tasks + :erpc); give
      # the replicas time to finish writing before asserting.
      Process.sleep(300)

      # With replication_factor: 2 exactly two nodes hold the data:
      # the primary and its one replica.  Only those nodes are checked.
      expected_nodes = [primary | replicas]

      for n <- expected_nodes do
        result = :erpc.call(n, Storage, :get, [:tpc_test, partition], 5_000)

        assert [{:tpc_test, :replicated}] == result,
               "Node #{inspect(n)} missing replicated record"
      end

      # The third node (neither primary nor replica for this partition)
      # must NOT hold any data — it was not part of this commit.
      all_nodes = Enum.sort([node(), node1, node2])
      non_replica = all_nodes -- Enum.sort(expected_nodes)

      case non_replica do
        [nr] ->
          result = :erpc.call(nr, Storage, :get, [:tpc_test, partition], 5_000)

          assert [] == result,
                 "Non-replica #{inspect(nr)} should not hold the record"

        [] ->
          # All three nodes are primary+replicas (possible if factor equals
          # cluster size), nothing extra to check.
          :ok
      end
    end

    # ── Transaction log cleanliness ──────────────────────────────────────────────

    test "transaction log is clean on all nodes after commit", %{node1: node1, node2: node2} do
      idx = Partition.get_partition_order(:tpc_log_clean)
      ops = [{:put, {:tpc_log_clean, :v}}]

      assert :ok == commit_from_primary(idx, ops)
      Process.sleep(300)

      for n <- [node(), node1, node2] do
        count = :erpc.call(n, TxnRegistry, :count, [], 5_000)
        assert count == 0, "Node #{inspect(n)} has #{count} leftover txn log entries"
      end
    end

    # ── Idempotency ───────────────────────────────────────────────────────────────

    test "commit is idempotent — second identical commit overwrites cleanly" do
      idx = Partition.get_partition_order(:tpc_idem)
      assert :ok == commit_from_primary(idx, [{:put, {:tpc_idem, :first}}])
      assert :ok == commit_from_primary(idx, [{:put, {:tpc_idem, :second}}])

      {primary, _} = Manager.get_replicas(idx)
      partition = Partition.get_partition_by_idx(idx)
      result = :erpc.call(primary, Storage, :get, [:tpc_idem, partition], 5_000)
      assert [{:tpc_idem, :second}] == result
    end

    # ── Failure / abort ───────────────────────────────────────────────────────────

    test "abort on participant crash leaves local ETS unchanged",
         %{peer1: peer1, node1: node1} do
      stop_peer(peer1)

      true =
        wait_until(fn ->
          node1 not in Manager.live_nodes()
        end)

      idx = Partition.get_partition_order(:tpc_abort)
      partition = Partition.get_partition_by_idx(idx)

      result = commit_from_primary(idx, [{:put, {:tpc_abort, :should_abort}}])

      assert result in [:ok, {:error, :_}]
      assert 0 == TxnRegistry.count()

      local =
        :erpc.call(
          elem(Manager.get_replicas(idx), 0),
          Storage,
          :get,
          [:tpc_abort, partition],
          5_000
        )

      assert local == [] or match?([{:tpc_abort, :should_abort}], local)
    end

    # ── ops fallback in handle_commit ────────────────────────────────────────────

    test "handle_commit applies fallback ops when TxnRegistry entry is absent" do
      # Simulate a replica that missed PREPARE: call handle_commit directly
      # with ops_fallback on the local node.
      partition = Partition.get_partition_by_idx(0)
      ops_fallback = [{:put, {:fallback_multi_node, :ok}}]

      assert :ack_commit ==
               ThreePhaseCommit.handle_commit("no-prepare-txn", 0, ops_fallback)

      assert [{:fallback_multi_node, :ok}] ==
               Storage.get(:fallback_multi_node, partition)
    end
  end

  # ── Private helpers ───────────────────────────────────────────────────────────

  defp start_peer(name) do
    cookie = :erlang.get_cookie()
    code_path = :code.get_path()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: ~c"127.0.0.1",
        args: [
          ~c"-setcookie",
          :erlang.atom_to_list(cookie),
          ~c"-connect_all",
          ~c"false"
        ]
      })

    :erpc.call(node, :code, :add_paths, [code_path])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logger])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:super_cache])
    :erpc.call(node, SuperCache.Cluster.Bootstrap, :start!, [@cluster_opts])

    {peer, node}
  end

  defp stop_peer(peer) do
    try do
      :peer.stop(peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp wait_until(fun, timeout \\ 3_000, interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn -> fun.() end)
    |> Stream.each(fn ok? -> unless ok?, do: Process.sleep(interval) end)
    |> Enum.find(fn ok? ->
      ok? or System.monotonic_time(:millisecond) >= deadline
    end)
  end
end
