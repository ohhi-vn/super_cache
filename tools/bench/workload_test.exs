#!/usr/bin/env elixir
# =============================================================================
# SuperCache Workload Test  (local + distributed)
#
# Simulates realistic access patterns and verifies correctness under load:
#   - Session cache (hot read, occasional write)
#   - Event queue (producer/consumer)
#   - Leaderboard (sorted struct store)
#   - Feature flags (KV, read-heavy)
#   - Cache stampede (many concurrent reads on the same key)
#   - Delete correctness
#
# Usage:
#   # local mode (default)
#   mix run tools/bench/workload_test.exs
#
#   # distributed mode
#   WORKLOAD_MODE=distributed mix run tools/bench/workload_test.exs
#
# Env vars:
#   WORKLOAD_MODE        "local" | "distributed"         (default: "local")
#   WORKLOAD_PEERS       comma-separated peer node names  (default: "")
#   WORKLOAD_DURATION_S  seconds per workload             (default: 10)
# =============================================================================

defmodule Workload.Config do
  @modes ~w(local distributed)

  def mode do
    raw = System.get_env("WORKLOAD_MODE", "local")
    unless raw in @modes do
      IO.puts("ERROR: WORKLOAD_MODE must be one of #{inspect(@modes)}, got: #{inspect(raw)}")
      System.halt(1)
    end
    String.to_atom(raw)
  end

  def distributed?, do: mode() == :distributed

  def peers do
    System.get_env("WORKLOAD_PEERS", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom/1)
  end

  def duration_ms do
    (System.get_env("WORKLOAD_DURATION_S", "10") |> String.to_integer()) * 1_000
  end

  # ---------------------------------------------------------------------------
  # Module resolution — returns the right implementation at runtime
  # ---------------------------------------------------------------------------

  def cache_mod,     do: pick(SuperCache.Distributed,           SuperCache)
  def queue_mod,     do: pick(SuperCache.Distributed.Queue,     SuperCache.Queue)
  def kv_mod,        do: pick(SuperCache.Distributed.KeyValue,  SuperCache.KeyValue)
  def struct_mod,    do: pick(SuperCache.Distributed.Struct,    SuperCache.Struct)

  defp pick(dist_mod, local_mod) do
    if distributed?(), do: dist_mod, else: local_mod
  end
end

# ── Helpers ──────────────────────────────────────────────────────────────────

defmodule Workload.Assert do
  def equal!(label, expected, actual) do
    if expected == actual do
      IO.puts("  ✓  #{label}")
    else
      IO.puts("  ✗  #{label}")
      IO.puts("     expected: #{inspect(expected)}")
      IO.puts("     actual:   #{inspect(actual)}")
    end
  end

  def ok!(label, true),  do: IO.puts("  ✓  #{label}")
  def ok!(label, false), do: IO.puts("  ✗  #{label}")

  def gt!(label, value, threshold) do
    if value > threshold do
      IO.puts("  ✓  #{label}  (#{value} > #{threshold})")
    else
      IO.puts("  ✗  #{label}  (#{value} not > #{threshold})")
    end
  end
end

defmodule Workload.Timer do
  def measure(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - t0
    {elapsed, result}
  end
end

defmodule Workload.Reporter do
  def section(title) do
    IO.puts("\n┌─────────────────────────────────────────────────────────────┐")
    IO.puts("│  #{String.pad_trailing(title, 59)}│")
    IO.puts("└─────────────────────────────────────────────────────────────┘")
  end

  def result(label, ops, duration_ms, errors \\ 0) do
    tp = Float.round(ops / max(duration_ms / 1_000, 0.001), 1)
    IO.puts("  #{String.pad_trailing(label, 30)}  ops=#{ops}  tp=#{tp}/s  errors=#{errors}  #{duration_ms}ms")
  end
end

defmodule Workload.Loop do
  @doc "Repeatedly calls `fun.(acc)` until the monotonic clock reaches `deadline_ms`."
  def loop_until(deadline_ms, acc, fun) do
    if System.monotonic_time(:millisecond) >= deadline_ms do
      acc
    else
      loop_until(deadline_ms, fun.(acc), fun)
    end
  end
end

# ── Workloads ─────────────────────────────────────────────────────────────────

defmodule Workload.SessionCache do
  @moduledoc """
  Simulates a session cache: 80% reads, 20% writes, 100 active sessions.
  Verifies that written sessions can be read back.
  """
  alias Workload.{Assert, Reporter, Loop}

  def run(duration_ms) do
    cache = Workload.Config.cache_mod()
    Reporter.section("Workload: Session Cache (80% read / 20% write) [#{cache}]")

    Enum.each(1..100, fn i ->
      cache.put!({:session, "tok-#{i}", %{user_id: i, role: :user, ts: System.os_time()}})
    end)

    deadline = System.monotonic_time(:millisecond) + duration_ms

    {elapsed, {ops, errors}} =
      Workload.Timer.measure(fn ->
        Loop.loop_until(deadline, {0, 0}, fn {ops, err} ->
          {result, _} =
            if :rand.uniform(5) == 1 do
              i = :rand.uniform(100)
              try do
                cache.put!({:session, "tok-#{i}", %{user_id: i, role: :user, ts: System.os_time()}})
                {:ok, nil}
              rescue
                _ -> {:error, nil}
              end
            else
              i = :rand.uniform(100)
              try do
                cache.get!({:session, "tok-#{i}", nil})
                {:ok, nil}
              rescue
                _ -> {:error, nil}
              end
            end

          case result do
            :ok    -> {ops + 1, err}
            :error -> {ops + 1, err + 1}
          end
        end)
      end)

    Reporter.result("session cache mixed", ops, elapsed, errors)

    missing =
      Enum.count(1..100, fn i ->
        cache.get!({:session, "tok-#{i}", nil}) == []
      end)

    Assert.equal!("all 100 sessions readable after load", 0, missing)

    sample_key = "tok-#{:rand.uniform(100)}"
    cache.put!({:session, sample_key, %{user_id: 999, role: :admin, ts: 0}})

    result =
      if Workload.Config.distributed?() do
        cache.get!({:session, sample_key, nil}, read_mode: :primary)
      else
        cache.get!({:session, sample_key, nil})
      end

    Assert.ok!("primary read returns written value", result != [])
  end
end

defmodule Workload.EventQueue do
  @moduledoc """
  Producer/consumer workload on a queue.
  Producers and consumers run concurrently; verifies no items are lost.
  """
  alias Workload.{Assert, Reporter}

  def run(duration_ms) do
    queue = Workload.Config.queue_mod()
    Reporter.section("Workload: Event Queue (concurrent producers + consumers) [#{queue}]")

    num_prod     = 5
    num_cons     = 5
    items_each   = 200
    task_timeout = duration_ms + 10_000
    deadline     = System.monotonic_time(:millisecond) + duration_ms

    prod_tasks =
      Enum.map(1..num_prod, fn p ->
        Task.async(fn ->
          Enum.each(1..items_each, fn i ->
            queue.add("workload_events", {p, i, System.os_time()})
          end)
          items_each
        end)
      end)

    cons_tasks =
      Enum.map(1..num_cons, fn _ ->
        Task.async(fn -> consumer_loop(queue, deadline, 0) end)
      end)

    all_results = Task.await_many(prod_tasks ++ cons_tasks, task_timeout)
    {prod_results, cons_results} = Enum.split(all_results, num_prod)

    total_produced = Enum.sum(prod_results)
    consumed       = Enum.sum(cons_results)
    remaining      = queue.count("workload_events")
    accounted      = consumed + remaining

    IO.puts("  produced: #{total_produced}  consumed: #{consumed}  remaining: #{remaining}")
    Reporter.result("queue producer/consumer", total_produced + consumed, duration_ms)
    Assert.equal!("produced + remaining = total_produced", total_produced, accounted)
    Assert.gt!("at least 50% consumed during run", consumed, div(total_produced, 2))
  end

  defp consumer_loop(queue, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      case queue.out("workload_events") do
        nil   -> Process.sleep(1); consumer_loop(queue, deadline, count)
        _item -> consumer_loop(queue, deadline, count + 1)
      end
    end
  end
end

defmodule Workload.Leaderboard do
  @moduledoc """
  Simulates a game leaderboard using the Struct store.
  Concurrent updates to the same player entries; verifies final state.
  """
  alias Workload.{Assert, Reporter}

  defmodule Player do
    defstruct [:id, :name, :score, :updated_at]
  end

  def run(duration_ms) do
    ds = Workload.Config.struct_mod()
    Reporter.section("Workload: Leaderboard (concurrent struct updates) [#{ds}]")

    ds.init(%Player{}, :id)

    Enum.each(1..50, fn i ->
      ds.add(%Player{id: i, name: "player_#{i}", score: 0, updated_at: System.os_time()})
    end)

    {elapsed, {ops, errors}} =
      Workload.Timer.measure(fn ->
        deadline = System.monotonic_time(:millisecond) + duration_ms

        tasks =
          Enum.map(1..10, fn _ ->
            Task.async(fn ->
              Stream.repeatedly(fn ->
                player_id = :rand.uniform(50)
                score     = :rand.uniform(1_000)

                try do
                  ds.add(%Player{
                    id:         player_id,
                    name:       "player_#{player_id}",
                    score:      score,
                    updated_at: System.os_time()
                  })
                  {:ok, nil}
                rescue
                  _ -> {:error, nil}
                end
              end)
              |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
              |> Enum.reduce({0, 0}, fn
                {:ok, _},    {o, e} -> {o + 1, e}
                {:error, _}, {o, e} -> {o + 1, e + 1}
              end)
            end)
          end)

        tasks
        |> Task.await_many(duration_ms + 5_000)
        |> Enum.reduce({0, 0}, fn {o, e}, {ao, ae} -> {ao + o, ae + e} end)
      end)

    Reporter.result("leaderboard updates", ops, elapsed, errors)

    {:ok, all} = ds.get_all(%Player{})
    Assert.equal!("50 players in store after concurrent updates", 50, length(all))

    {:ok, p} = ds.get(%Player{id: 1})
    Assert.ok!("player score is a positive integer", is_integer(p.score) and p.score >= 0)
  end
end

defmodule Workload.FeatureFlags do
  @moduledoc """
  Read-heavy KV workload simulating feature flags.
  """
  alias Workload.{Assert, Reporter}

  @flags [:dark_mode, :new_checkout, :beta_search, :turbo_mode, :legacy_api]

  def run(duration_ms) do
    kv = Workload.Config.kv_mod()
    Reporter.section("Workload: Feature Flags (read-heavy KV) [#{kv}]")

    Enum.each(@flags, &kv.add("flags", &1, false))
    kv.add("flags", :dark_mode,    true)
    kv.add("flags", :new_checkout, true)

    deadline = System.monotonic_time(:millisecond) + duration_ms

    {elapsed, {reads, writes, errors}} =
      Workload.Timer.measure(fn ->
        tasks =
          Enum.map(1..20, fn worker ->
            Task.async(fn ->
              Stream.repeatedly(fn ->
                if :rand.uniform(20) == 1 and worker == 1 do
                  flag = Enum.random(@flags)
                  val  = :rand.uniform(2) == 1
                  try do
                    kv.add("flags", flag, val)
                    {:write, :ok}
                  rescue
                    _ -> {:write, :error}
                  end
                else
                  flag = Enum.random(@flags)
                  try do
                    kv.get("flags", flag)
                    {:read, :ok}
                  rescue
                    _ -> {:read, :error}
                  end
                end
              end)
              |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
              |> Enum.reduce({0, 0, 0}, fn
                {:read,  :ok},  {r, w, e} -> {r + 1, w,     e}
                {:write, :ok},  {r, w, e} -> {r,     w + 1, e}
                {_,      :error},{r, w, e} -> {r,     w,     e + 1}
              end)
            end)
          end)

        tasks
        |> Task.await_many(duration_ms + 5_000)
        |> Enum.reduce({0, 0, 0}, fn {r, w, e}, {ar, aw, ae} ->
          {ar + r, aw + w, ae + e}
        end)
      end)

    Reporter.result("feature flags reads",  reads,  elapsed, 0)
    Reporter.result("feature flags writes", writes, elapsed, 0)
    Reporter.result("feature flags errors", errors, elapsed, errors)

    missing = Enum.count(@flags, fn f -> kv.get("flags", f) == nil end)
    Assert.equal!("all feature flags readable after load", 0, missing)
    Assert.ok!("count returns correct number of flags", kv.count("flags") == length(@flags))
  end
end

defmodule Workload.Stampede do
  @moduledoc """
  Cache stampede: hot key under concurrent writers and readers.
  """
  alias Workload.{Assert, Reporter}

  def run(duration_ms) do
    cache = Workload.Config.cache_mod()
    Reporter.section("Workload: Cache Stampede (hot key contention) [#{cache}]")

    hot_key = "stampede-hot"
    cache.put!({:stamp, hot_key, "v0"})

    deadline = System.monotonic_time(:millisecond) + duration_ms

    writer =
      Task.async(fn ->
        Stream.iterate(1, &(&1 + 1))
        |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
        |> Enum.each(fn v ->
          cache.put!({:stamp, hot_key, "v#{v}"})
          Process.sleep(5)
        end)
      end)

    readers =
      Enum.map(1..50, fn _ ->
        Task.async(fn ->
          Stream.repeatedly(fn ->
            cache.get!({:stamp, hot_key, nil})
          end)
          |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
          |> Enum.reduce({0, 0}, fn
            [],    {ok, miss} -> {ok,     miss + 1}
            [_|_], {ok, miss} -> {ok + 1, miss}
          end)
        end)
      end)

    Task.await(writer, duration_ms + 5_000)
    {total_hits, total_miss} =
      readers
      |> Task.await_many(duration_ms + 5_000)
      |> Enum.reduce({0, 0}, fn {h, m}, {ah, am} -> {ah + h, am + m} end)

    Reporter.result("stampede reads (hit)",  total_hits, duration_ms)
    Reporter.result("stampede reads (miss)", total_miss, duration_ms)

    result =
      if Workload.Config.distributed?() do
        cache.get!({:stamp, hot_key, nil}, read_mode: :primary)
      else
        cache.get!({:stamp, hot_key, nil})
      end

    Assert.ok!("hot key still exists after stampede", result != [])
    Assert.ok!("hot key value is a string", match?([{:stamp, ^hot_key, "v" <> _}], result))
  end
end

defmodule Workload.DeleteCorrectness do
  @moduledoc """
  Verifies that delete and delete_all behave correctly under concurrent writes.
  """
  alias Workload.{Assert, Reporter}

  def run do
    cache = Workload.Config.cache_mod()
    Reporter.section("Workload: Delete Correctness [#{cache}]")

    Enum.each(1..200, fn i ->
      cache.put!({{:deltest, i}, "val-#{i}"})
    end)

    Enum.each(1..100, fn i ->
      cache.delete!({{:deltest, i}, nil})
    end)

    read = fn i ->
      if Workload.Config.distributed?() do
        cache.get!({{:deltest, i}, nil}, read_mode: :primary)
      else
        cache.get!({{:deltest, i}, nil})
      end
    end

    still_present = Enum.count(1..100, fn i -> read.(i) != [] end)
    Assert.equal!("individually deleted records are gone", 0, still_present)

    still_present2 = Enum.count(101..200, fn i -> read.(i) != [] end)
    Assert.equal!("non-deleted records are still present", 100, still_present2)

    cache.delete_all()
    Process.sleep(200)

    any_left = Enum.any?(101..200, fn i -> read.(i) != [] end)
    Assert.equal!("delete_all clears all records", false, any_left)
  end
end

# =============================================================================
# Main runner
# =============================================================================

defmodule Workload.Runner do
  def run do
    mode = Workload.Config.mode()

    IO.puts("""

    ╔══════════════════════════════════════════════════════════════╗
    ║          SuperCache Workload Test                            ║
    ╚══════════════════════════════════════════════════════════════╝

    mode     : #{mode}
    peers    : #{inspect(Workload.Config.peers())}
    duration : #{div(Workload.Config.duration_ms(), 1_000)}s per workload
    """)

    boot(mode)

    dur = Workload.Config.duration_ms()
    cache = Workload.Config.cache_mod()

    Workload.SessionCache.run(dur)
    cache.delete_all(); Process.sleep(200)

    Workload.EventQueue.run(dur)
    cache.delete_all(); Process.sleep(200)

    Workload.Leaderboard.run(dur)
    cache.delete_all(); Process.sleep(200)

    Workload.FeatureFlags.run(dur)
    cache.delete_all(); Process.sleep(200)

    Workload.Stampede.run(dur)
    cache.delete_all(); Process.sleep(200)

    Workload.DeleteCorrectness.run()
    cache.delete_all()

    Workload.Reporter.section("Post-Workload Stats")

    if Workload.Config.distributed?() do
      SuperCache.Cluster.Stats.cluster()            |> SuperCache.Cluster.Stats.print()
      SuperCache.Cluster.Stats.three_phase_commit() |> SuperCache.Cluster.Stats.print()
      SuperCache.Cluster.Stats.api()                |> SuperCache.Cluster.Stats.print()
    else
      SuperCache.stats() |> IO.inspect(label: "stats")
    end

    IO.puts("\nWorkload test complete.\n")
    shutdown(mode)
  end

  # ---------------------------------------------------------------------------
  # Bootstrap helpers
  # ---------------------------------------------------------------------------

  defp boot(:distributed) do
    if SuperCache.started?(), do: SuperCache.Cluster.Bootstrap.stop()
    Process.sleep(100)

    SuperCache.Cluster.Bootstrap.start!(
      key_pos:            0,
      partition_pos:      0,
      cluster:            :distributed,
      replication_factor: 2,
      replication_mode:   :async,
      num_partition:      8,
      table_type:         :set
    )

    Enum.each(Workload.Config.peers(), fn peer ->
      case Node.connect(peer) do
        true     -> IO.puts("  connected → #{peer}")
        false    -> IO.puts("  WARN: could not connect to #{peer}")
        :ignored -> IO.puts("  WARN: node not alive #{peer}")
      end
    end)

    if Workload.Config.peers() != [], do: Process.sleep(1_500)
  end

  defp boot(:local) do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(100)

    SuperCache.start!(
      key_pos:       0,
      partition_pos: 0,
      num_partition: 8,
      table_type:    :set
    )
  end

  defp shutdown(:distributed), do: SuperCache.Cluster.Bootstrap.stop()
  defp shutdown(:local),        do: SuperCache.stop()
end

Workload.Runner.run()
