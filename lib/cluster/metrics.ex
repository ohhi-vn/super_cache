defmodule SuperCache.Cluster.Metrics do
  @moduledoc """
  Low-overhead counter and latency-sample store for SuperCache observability.

  All state is held in a single `:public` ETS table owned by this GenServer.
  Writes use `update_counter/3` (atomic) and are safe to call from any
  process without coordination overhead.

  ## Counter keys

  Counters are stored as `{namespace, field}` tuples:

      {:tpc, :committed}
      {:tpc, :aborted}
      {:api, :put}, :calls}          # note: nested via get_all/1
      {{:api, :put}, :errors}

  ## Latency samples

  Latency samples use a fixed-size ring buffer written directly by the
  caller — `push_latency/2` performs one atomic counter update and one
  `:ets.insert/2`, with no `GenServer` message and no intermediate list
  allocation.  Each key stores at most 256 samples in slots
  keyed `{:latency, key, slot}`; the oldest slot is overwritten when the
  ring wraps, so memory per key is constant regardless of call volume.

  Concurrent writers are safe: sequence numbers come from
  `:ets.update_counter/4`, so each sample lands in its own slot and the
  buffer never grows past the cap.  A write that races with another may
  overwrite a slot, which only drops a sample; it cannot corrupt counters
  or produce unbounded state.

  `get_latency_samples/1` returns the retained values newest-first.

  ## Usage

      # In Router / Replicator — after an operation completes:
      Metrics.increment({:api, :put}, :calls)
      Metrics.push_latency({:api_latency_us, :put}, elapsed_us)

      # Reading back:
      Metrics.get_all({:api, :put})
      # => %{calls: 1_200, errors: 2}

      Metrics.get_latency_samples({:api_latency_us, :put})
      # => [45, 110, 88, ...]
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  @table __MODULE__
  @max_samples 256

  alias SuperCache.Storage.MatchSpec

  # ── Lifecycle ────────────────────────────────────────────────────────────────

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    @table =
      :ets.new(@table, [
        :set,
        :public,
        :named_table,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])

    {:ok, %{}}
  end

  # ── Counter API ───────────────────────────────────────────────────────────────

  @doc "Atomically increment counter `field` under `namespace` by `amount` (default 1)."
  @spec increment(term, atom, integer) :: integer
  def increment(namespace, field, amount \\ 1) when is_integer(amount) do
    :ets.update_counter(@table, {namespace, field}, {2, amount}, {{namespace, field}, 0})
  end

  @doc "Return all counters for `namespace` as `%{field => count}`.\n  Uses compiled match spec for better performance.\n  "
  @spec get_all(term) :: map
  def get_all(namespace) do
    spec = MatchSpec.match({{namespace, :"$1"}, :"$2"}, [:"$1", :"$2"])

    MatchSpec.select(@table, spec)
    |> Map.new(fn [field, count] -> {field, count} end)
  end

  @doc "Reset all counters and latency samples for `namespace`.\n  Uses compiled match specs for better performance.\n  "
  @spec reset(term) :: :ok
  def reset(namespace) do
    specs = [
      # Counters.
      MatchSpec.delete_match({{namespace, :_}, :_}),
      # Ring-buffer slots, both `namespace` and `{namespace, op}` keys.
      MatchSpec.delete_match({{:latency, namespace, :_}, :_}),
      MatchSpec.delete_match({{:latency, {namespace, :_}, :_}, :_}),
      # Sequence counters that place samples into slots.
      MatchSpec.delete_match({{:latency_seq, namespace, :seq}, :_}),
      MatchSpec.delete_match({{:latency_seq, {namespace, :_}, :seq}, :_}),
      # Legacy list-shaped buffers from older releases.
      MatchSpec.delete_match({{:latency, namespace}, :_}),
      MatchSpec.delete_match({{:latency, {namespace, :_}}, :_})
    ]

    Enum.each(specs, &MatchSpec.select_delete(@table, &1))
    :ok
  end

  # ── Latency sample API ────────────────────────────────────────────────────────

  @doc """
  Push a latency sample (microseconds) into the ring buffer for `key`.

  The sample is written synchronously by the calling process.  The buffer
  is capped at #{@max_samples} entries; oldest samples are dropped when the
  cap is reached.
  """
  @spec push_latency(term, non_neg_integer) :: :ok
  def push_latency(key, value_us) when is_integer(value_us) do
    seq_key = {:latency_seq, key, :seq}
    seq = :ets.update_counter(@table, seq_key, {2, 1}, {seq_key, 0})
    slot = rem(seq, @max_samples)
    :ets.insert(@table, {{:latency, key, slot}, {seq, value_us}})
    :ok
  end

  @doc "Return all retained latency samples for `key`, newest first."
  @spec get_latency_samples(term) :: [non_neg_integer]
  def get_latency_samples(key) do
    spec = MatchSpec.match({{:latency, key, :_}, {:"$1", :"$2"}}, [:"$1", :"$2"])

    @table
    |> MatchSpec.select(spec)
    |> Enum.sort_by(fn [seq, _value] -> seq end, :desc)
    |> Enum.map(fn [_seq, value] -> value end)
  end

  # ── GenServer callbacks ───────────────────────────────────────────────────────

  @impl true
  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end
