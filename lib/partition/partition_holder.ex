defmodule SuperCache.Partition.Holder do
  @moduledoc """
  Registry for partition ETS table names.

  Maintains a mapping from partition index (integer) to the corresponding
  ETS table atom.  The mapping is stored in a `:protected` ETS table owned
  by this GenServer so that:

  - Reads are lock-free (`:read_concurrency`).
  - The table survives crashes as long as this GenServer is alive.
  - Multiple processes can look up partitions concurrently without
    blocking each other.

  ## Lifecycle

  1. On application start, `Partition.start/1` calls `set_num_partition/1`
     and `set/1` for each index to populate the registry.
  2. During runtime, `get/1` and `get_all/1` are used by the routing layer
     to resolve partition tables.
  3. On shutdown, `stop/0` clears the registry.

  ## Example

      SuperCache.Partition.Holder.set_num_partition(4)
      SuperCache.Partition.Holder.set(0)
      SuperCache.Partition.Holder.get(0)
      # => :"SuperCache.Storage.Ets_0"
  """

  use GenServer, restart: :transient, shutdown: 1_000

  require Logger
  require SuperCache.Log

  alias SuperCache.Config

  ## ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Starts the Partition.Holder GenServer and its owned ETS table.
  """
  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stops the Partition.Holder GenServer.

  The ETS table will be deleted when the GenServer terminates.
  """
  @spec stop() :: :ok
  def stop() do
    GenServer.call(__MODULE__, :stop, 5_000)
  end

  @doc """
  Clears all partition registrations from the ETS table.

  Does NOT delete the ETS table itself — only removes its contents.
  """
  @spec clean() :: true
  def clean() do
    GenServer.call(__MODULE__, :clean, 5_000)
  end

  @doc """
  Register a partition index with its corresponding ETS table name.

  The table name is derived from the configured `:table_prefix` and the
  given `order` (index).  For example, with prefix `"SuperCache.Storage.Ets"`
  and order `2`, the table name will be `:"SuperCache.Storage.Ets_2"`.
  """
  @spec set(non_neg_integer) :: :ok
  def set(order) when is_integer(order) and order >= 0 do
    GenServer.call(__MODULE__, {:set_partition, order}, 5_000)
  end

  @doc """
  Look up the ETS table atom for a given partition index.

  Reads directly from the ETS table — no GenServer hop.
  Raises if the index has not been registered.

  ## Examples

      SuperCache.Partition.Holder.get(0)
      # => :"SuperCache.Storage.Ets_0"
  """
  @doc """
  Look up the ETS table atom for a given partition index.

  Reads directly from the ETS table — no GenServer hop.
  Returns `nil` if the index has not been registered yet (e.g., during startup races).
  """
  @spec get(non_neg_integer) :: atom | nil
  def get(order) when is_integer(order) and order >= 0 do
    case :ets.lookup(__MODULE__, order) do
      [{^order, partition}] -> partition
      [] -> nil
    end
  end

  @doc """
  Return all registered partition ETS table atoms.

  Filters out non-partition entries (such as `:num_partition`).
  """
  @spec get_all() :: [atom]
  def get_all() do
    :ets.match(__MODULE__, {:"$1", :"$2"})
    |> Enum.filter(fn [k, _v] -> is_integer(k) end)
    |> Enum.map(fn [_k, v] -> v end)
  end

  @doc """
  Store the total number of partitions in the registry.

  Used by `Partition.get_num_partition/0` to resolve the partition count
  without a GenServer hop.
  """
  @spec set_num_partition(pos_integer) :: :ok
  def set_num_partition(num) when is_integer(num) and num > 0 do
    GenServer.call(__MODULE__, {:set_num_partition, num}, 5_000)
  end

  ## ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    table_name = __MODULE__

    ^table_name =
      :ets.new(table_name, [
        :set,
        :protected,
        :named_table,
        {:read_concurrency, true},
        {:decentralized_counters, true}
      ])

    Logger.info("super_cache, partition_holder, ETS table #{inspect(table_name)} created")
    {:ok, %{table_name: table_name}}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    Logger.info("super_cache, partition_holder, stopping")
    {:stop, :normal, :ok, state}
  end

  def handle_call(:clean, _from, %{table_name: table_name} = state) do
    SuperCache.Log.debug(fn -> "super_cache, partition_holder, clearing all entries" end)
    {:reply, clean_up(table_name), state}
  end

  def handle_call({:set_num_partition, num}, _from, %{table_name: table_name} = state) do
    SuperCache.Log.debug(fn ->
      "super_cache, partition_holder, set num_partition = #{num}"
    end)

    :ets.insert(table_name, {:num_partition, :config, num})
    {:reply, :ok, state}
  end

  def handle_call({:set_partition, order}, _from, %{table_name: table_name} = state) do
    prefix = Config.get_config(:table_prefix)
    partition = String.to_atom("#{prefix}_#{order}")

    SuperCache.Log.debug(fn ->
      "super_cache, partition_holder, registered index #{order} → #{inspect(partition)}"
    end)

    :ets.insert(table_name, {order, partition})
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, _state) do
    Logger.info("super_cache, partition_holder, shutting down (reason: #{inspect(reason)})")

    :ok
  end

  ## ── Private helpers ──────────────────────────────────────────────────────────

  @spec clean_up(atom) :: true
  defp clean_up(table_name) do
    :ets.delete_all_objects(table_name)
  end
end
