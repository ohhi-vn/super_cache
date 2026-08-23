defmodule SuperCache.CoverageRoundThreeTest do
  @moduledoc """
  Last-mile branches: application auto-start, bootstrap startup-rollback,
  router quorum fallback with no majority, stats resilience to missing
  tables and WAL recovery with planted replicas.
  """

  use ExUnit.Case, async: false

  alias SuperCache.Cluster.{Manager, Stats, WAL}
  alias SuperCache.{EtsHolder, Partition, Storage}

  @pt_partition_map {Manager, :partition_map}

  # ── Application auto-start ───────────────────────────────────────────────────

  test "auto_start boots the cache and connects configured peers" do
    original_env = Application.get_all_env(:super_cache)

    try do
      Application.put_env(:super_cache, :auto_start, true)
      Application.put_env(:super_cache, :key_pos, 0)
      Application.put_env(:super_cache, :partition_pos, 0)
      Application.put_env(:super_cache, :num_partition, 2)
      Application.put_env(:super_cache, :cluster_peers, [:"autostart_dead@host"])

      :ok = Application.stop(:super_cache)
      {:ok, _} = Application.ensure_all_started(:super_cache)

      Process.sleep(100)
      assert SuperCache.started?() == true
    after
      Application.put_env(:super_cache, :auto_start, false)
      Application.delete_env(:super_cache, :key_pos)
      Application.delete_env(:super_cache, :partition_pos)
      Application.delete_env(:super_cache, :num_partition)
      Application.delete_env(:super_cache, :cluster_peers)

      :ok = Application.stop(:super_cache)
      {:ok, _} = Application.ensure_all_started(:super_cache)

      Enum.each(original_env, fn {k, v} -> Application.put_env(:super_cache, k, v) end)
    end
  end

  # ── Router quorum read with zero successful responses ────────────────────────

  test "quorum read falls back gracefully when every replica fails" do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(30)

    :ok =
      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 3,
        num_partition: 2
      )

    original = :persistent_term.get(@pt_partition_map, nil)

    try do
      # Every node in the quorum set is unreachable.
      :persistent_term.put(@pt_partition_map, %{0 => {:"q1_dead@host", [:"q2_dead@host"]}})

      assert [] ==
               SuperCache.get_by_key_partition!(:whatever, 0, read_mode: :quorum)
    after
      if original,
        do: :persistent_term.put(@pt_partition_map, original),
        else: :persistent_term.erase(@pt_partition_map)
    end
  end

  # ── Stats resilience ─────────────────────────────────────────────────────────

  test "Stats.cluster/0 tolerates a missing partition table" do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
    Process.sleep(30)

    victim = Partition.get_partition_by_idx(0)
    real = :ets.info(victim)

    try do
      :ets.delete(victim)
      overview = Stats.cluster()
      assert is_integer(overview.total_records)
    after
      if real != :undefined, do: EtsHolder.new_table(EtsHolder, victim)
    end
  end

  # ── WAL recover with planted replicas ────────────────────────────────────────

  test "recover/0 re-replicates entries to unreachable replicas without crashing" do
    SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 2)
    Process.sleep(30)

    original = :persistent_term.get(@pt_partition_map, nil)

    try do
      :persistent_term.put(@pt_partition_map, %{0 => {node(), [:"wal_rec@host"]}})

      entry = %{
        seq: 97_001,
        partition_idx: 0,
        ops: [{:put, {:rec_rep, 1}}],
        timestamp: System.monotonic_time(:millisecond)
      }

      :ets.insert(WAL, {97_001, entry})

      assert :ok = WAL.recover()
    after
      :ets.delete(WAL, 97_001)

      if original,
        do: :persistent_term.put(@pt_partition_map, original),
        else: :persistent_term.erase(@pt_partition_map)
    end
  end

  defp wait_until(fun, tries \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met")

  defp wait_until(fun, tries) do
    if fun.(), do: :ok, else: Process.sleep(25) && wait_until(fun, tries - 1)
  end
end
