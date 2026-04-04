defmodule SuperCache.Partition do
  @moduledoc """
  Partition resolution and management for SuperCache.

  SuperCache splits data across multiple ETS tables (partitions) to reduce
  contention and improve parallelism. This module handles:

  - **Hashing** — mapping arbitrary terms to partition indices using `:erlang.phash2/2`.
  - **Resolution** — translating partition indices to ETS table atoms.
  - **Lifecycle** — initialising and tearing down the partition registry.

  ## How partitioning works

  1. A term (usually extracted from a tuple via `:partition_pos`) is hashed
     with `:erlang.phash2/2` modulo the number of partitions.
  2. The resulting index is looked up in `SuperCache.Partition.Holder` to
     get the corresponding ETS table atom.
  3. All reads and writes for that term go to that table.

  ## Example

      # Start with 4 partitions
      SuperCache.Partition.start(4)

      # Resolve partition for a term
      SuperCache.Partition.get_partition(:user_123)
      # => :"SuperCache.Storage.Ets_2"

      # Get partition index directly
      SuperCache.Partition.get_partition_order(:user_123)
      # => 2

      # Clean up
      SuperCache.Partition.stop()
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Partition.Holder

  # Cache num_partition in persistent_term for O(1) reads on the hot path.
  @pt_num_partition {__MODULE__, :num_partition}

  # Cache partition table atoms in a tuple for O(1) indexed access.
  # Eliminates the ETS lookup in Holder.get/1 on every partition resolution.
  @pt_partitions {__MODULE__, :partition_tables}

  # ─── Partition resolution ───────────────────────────────────────────────────

  @doc """
  Resolve the ETS table atom for a given term.

  The term is hashed to derive a partition index, which is then looked up
  in the `Holder` registry to return the corresponding ETS table name.

  This is the primary entry point used by the public API when the caller
  has a raw key or partition value.

  ## Examples

      SuperCache.Partition.get_partition(:session_abc)
      # => :"SuperCache.Storage.Ets_1"

      SuperCache.Partition.get_partition({:user, 42})
      # => :"SuperCache.Storage.Ets_3"
  """
  @spec get_partition(any) :: atom
  @compile {:inline, get_partition: 1}
  def get_partition(data) do
    idx = :erlang.phash2(data, fast_num_partition())
    fast_get_partition_by_idx(idx)
  end

  @doc """
  Resolve the ETS table atom directly from a partition index.

  Does **not** re-hash the input. Safe to call from the Replicator and
  Router where the index is already known.

  ## Examples

      SuperCache.Partition.get_partition_by_idx(0)
      # => :"SuperCache.Storage.Ets_0"
  """
  @spec get_partition_by_idx(non_neg_integer) :: atom | nil
  def get_partition_by_idx(idx) when is_integer(idx) and idx >= 0 do
    fast_get_partition_by_idx(idx)
  end

  # Fast path: read partition table atom from cached tuple in persistent_term.
  # Falls back to ETS lookup if cache not yet built (early boot).
  # Returns `nil` if the partition index is not registered.
  @compile {:inline, fast_get_partition_by_idx: 1}
  defp fast_get_partition_by_idx(idx) do
    case :persistent_term.get(@pt_partitions, nil) do
      nil ->
        # Cache not ready — fall back to ETS lookup.
        Holder.get(idx)

      tuple when is_tuple(tuple) and idx < tuple_size(tuple) ->
        elem(tuple, idx)

      _tuple ->
        # Index out of bounds — fall back to ETS lookup.
        Holder.get(idx)
    end
  end

  # ─── Index helpers ──────────────────────────────────────────────────────────

  @doc """
  Compute the partition index for any term.

  Uses `:erlang.phash2/2` with the configured number of partitions as the
  range. The result is an integer in `0..(num_partitions - 1)`.

  ## Examples

      SuperCache.Partition.get_partition_order(:hello)
      # => 2
  """
  @spec get_partition_order(any) :: non_neg_integer
  @compile {:inline, get_partition_order: 1}
  def get_partition_order(term) do
    :erlang.phash2(term, fast_num_partition())
  end

  @doc """
  Compute the partition index for a data tuple using the configured `:partition_pos`.

  Extracts the element at `:partition_pos` from the tuple, then hashes it
  to determine the partition index.

  ## Examples

      SuperCache.Config.set_config(:partition_pos, 1)
      SuperCache.Partition.get_partition_order_from_data({:user, 42, "data"})
      # => (hash of 42) mod num_partitions
  """
  @spec get_partition_order_from_data(tuple) :: non_neg_integer
  def get_partition_order_from_data(data) when is_tuple(data) do
    data
    |> SuperCache.Config.get_partition!()
    |> get_partition_order()
  end

  # ─── Cluster / management ───────────────────────────────────────────────────

  @doc """
  List all registered partition ETS table atoms.

  Returns a flat list of table names, excluding internal registry entries.

  ## Examples

      SuperCache.Partition.start(3)
      SuperCache.Partition.get_all_partition()
      # => [:"SuperCache.Storage.Ets_0", :"SuperCache.Storage.Ets_1", :"SuperCache.Storage.Ets_2"]
  """
  @spec get_all_partition() :: [atom]
  def get_all_partition(), do: Holder.get_all()

  @doc """
  Initialise the partition registry.

  Stores the total partition count and registers each index (0 to N-1)
  with its corresponding ETS table name.

  Called by `SuperCache.Bootstrap` during startup.

  ## Examples

      SuperCache.Partition.start(4)
      # => :ok
  """
  @spec start(pos_integer) :: :ok
  def start(num_partition) when is_integer(num_partition) do
    SuperCache.Log.debug(fn ->
      "super_cache, partition, initialising #{num_partition} partition(s)"
    end)

    # Cache num_partition in persistent_term for fast hot-path reads.
    :persistent_term.put(@pt_num_partition, num_partition)

    Holder.set_num_partition(num_partition)

    # Build partition table name tuple for O(1) indexed access.
    prefix = SuperCache.Config.get_config(:table_prefix, "SuperCache.Storage.Ets")

    partition_tuple =
      0..(num_partition - 1)
      |> Enum.map(fn order ->
        table_name = String.to_atom("#{prefix}_#{order}")
        Holder.set(order)
        table_name
      end)
      |> List.to_tuple()

    # Cache the tuple in persistent_term — safe because atoms are built at startup only.
    :persistent_term.put(@pt_partitions, partition_tuple)

    Logger.info("super_cache, partition, #{num_partition} partition(s) registered")
    :ok
  end

  @doc """
  Clear all partition registrations from the registry.

  Does **not** delete the ETS tables themselves — only removes the
  index-to-name mappings from the `Holder`.

  Called by `SuperCache.Bootstrap` during shutdown.
  """
  @spec stop() :: :ok
  def stop() do
    SuperCache.Log.debug(fn -> "super_cache, partition, clearing registry" end)
    :persistent_term.erase(@pt_num_partition)
    :persistent_term.erase(@pt_partitions)
    Holder.clean()
    :ok
  end

  @doc """
  Return the configured number of partitions.

  Reads directly from the `Holder` ETS table — no GenServer hop.
  Raises if the partition count has not been initialised.

  ## Examples

      SuperCache.Partition.start(8)
      SuperCache.Partition.get_num_partition()
      # => 8
  """
  @spec get_num_partition() :: pos_integer
  def get_num_partition() do
    case :ets.lookup(Holder, :num_partition) do
      [{:num_partition, :config, num}] when is_integer(num) and num > 0 ->
        num

      [] ->
        raise "partition count not initialised. Call Partition.start/1 first."

      other ->
        raise "unexpected partition count entry: #{inspect(other)}"
    end
  end

  # Fast path: read num_partition from persistent_term (set during start/1).
  # Falls back to ETS lookup if not yet cached (early boot).
  @compile {:inline, fast_num_partition: 0}
  defp fast_num_partition() do
    case :persistent_term.get(@pt_num_partition, :__not_set__) do
      :__not_set__ ->
        n = get_num_partition()
        :persistent_term.put(@pt_num_partition, n)
        n

      n ->
        n
    end
  end

  @doc """
  Return the number of online schedulers in the Erlang VM.

  Used as the default partition count when `:num_partition` is not
  explicitly configured.

  ## Examples

      SuperCache.Partition.get_schedulers()
      # => 8  (on an 8-core machine)
  """
  @spec get_schedulers() :: pos_integer
  def get_schedulers(), do: System.schedulers_online()

  # ─── Deprecated / backwards compatibility ───────────────────────────────────

  @doc false
  @deprecated "Use get_partition_order/1 instead"
  @spec get_hash(any) :: non_neg_integer
  def get_hash(term), do: :erlang.phash2(term)

  @doc false
  @deprecated "Use get_partition_order/1 instead"
  @spec get_partition_order(any) :: non_neg_integer
  def get_pattition_order(term), do: get_partition_order(term)
end
