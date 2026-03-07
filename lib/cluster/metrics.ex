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

  Latency ring buffers are stored as `{:latency, key}` records holding a
  list of the most recent `@max_samples` microsecond values.  The list is
  bounded so memory usage is constant regardless of call volume.

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

  @table    __MODULE__
  @max_samples 256

  # ── Lifecycle ────────────────────────────────────────────────────────────────

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    @table = :ets.new(@table, [
      :set,
      :public,
      :named_table,
      {:write_concurrency, true},
      {:read_concurrency, true}
    ])

    {:ok, %{}}
  end

  # ── Counter API ───────────────────────────────────────────────────────────────

  @doc "Atomically increment counter `field` under `namespace` by 1."
  @spec increment(term, atom) :: integer
  def increment(namespace, field) do
    :ets.update_counter(@table, {namespace, field}, {2, 1}, {{namespace, field}, 0})
  end

  @doc "Return all counters for `namespace` as `%{field => count}`."
  @spec get_all(term) :: map
  def get_all(namespace) do
    :ets.match(@table, {{namespace, :"$1"}, :"$2"})
    |> Map.new(fn [field, count] -> {field, count} end)
  end

  @doc "Reset all counters and latency samples for `namespace`."
  @spec reset(term) :: :ok
  def reset(namespace) do
    :ets.match_delete(@table, {{namespace, :_}, :_})
    :ets.match_delete(@table, {{:latency, namespace}, :_})
    :ok
  end

  # ── Latency sample API ────────────────────────────────────────────────────────

  @doc """
  Push a latency sample (microseconds) into the ring buffer for `key`.

  The buffer is capped at #{@max_samples} entries; oldest samples are
  dropped when the cap is reached.
  """
  @spec push_latency(term, non_neg_integer) :: :ok
  def push_latency(key, value_us) when is_integer(value_us) do
    GenServer.cast(__MODULE__, {:push_latency, key, value_us})
  end

  @doc "Return all latency samples for `key` (unordered)."
  @spec get_latency_samples(term) :: [non_neg_integer]
  def get_latency_samples(key) do
    case :ets.lookup(@table, {:latency, key}) do
      [{_, samples}] -> samples
      []             -> []
    end
  end

  # ── GenServer callbacks ───────────────────────────────────────────────────────

  @impl true
  def handle_cast({:push_latency, key, value}, state) do
    ets_key = {:latency, key}

    existing =
      case :ets.lookup(@table, ets_key) do
        [{_, s}] -> s
        []       -> []
      end

    updated =
      if length(existing) >= @max_samples do
        [value | Enum.drop(existing, -1)]
      else
        [value | existing]
      end

    :ets.insert(@table, {ets_key, updated})
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end
