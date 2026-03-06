defmodule SuperCache.Partition do
  @moduledoc false

  alias SuperCache.Partition.Holder
  alias :ets, as: Ets

  # ─── Partition resolution ───────────────────────────────────────────────────

  @doc """
  Resolve a partition ETS table name from **partition data** (any term).
  The data is hashed to derive the partition index, then the index is looked
  up in the Holder ETS table.
  Used by the public API when the caller has the raw key/partition value.
  """
  @spec get_partition(any) :: atom
  def get_partition(data) do
    data
    |> get_partition_order()
    |> get_partition_by_idx()
  end

  @doc """
  Resolve a partition ETS table name directly from a **partition index**.
  Does NOT re-hash — safe to call from the Replicator and Router where the
  index is already known.
  """
  @spec get_partition_by_idx(non_neg_integer) :: atom
  def get_partition_by_idx(idx) when is_integer(idx) and idx >= 0 do
    Holder.get(idx)
  end

  # ─── Index helpers ──────────────────────────────────────────────────────────

  @doc "Compute the partition index for any term (hash mod num_partitions)."
  @spec get_partition_order(any) :: non_neg_integer
  def get_partition_order(term) do
    :erlang.phash2(term, get_num_partition())
  end

  @doc "Compute the partition index for a data tuple using configured partition_pos."
  @spec get_partition_order_from_data(tuple) :: non_neg_integer
  def get_partition_order_from_data(data) when is_tuple(data) do
    data
    |> SuperCache.Config.get_partition!()
    |> get_partition_order()
  end

  # ─── Cluster / management ───────────────────────────────────────────────────

  @doc "List all partition ETS table names."
  @spec get_all_partition() :: [atom]
  def get_all_partition(), do: Holder.get_all()

  @doc "Initialise partitions: store the count and register each index → name."
  @spec start(pos_integer) :: :ok
  def start(num_partition) when is_integer(num_partition) do
    Holder.set_num_partition(num_partition)
    for order <- 0..(num_partition - 1), do: Holder.set(order)
    :ok
  end

  @doc "Clear all partition registrations."
  @spec stop() :: :ok
  def stop() do
    Holder.clean()
    :ok
  end

  @doc "Return the configured number of partitions."
  @spec get_num_partition() :: pos_integer
  def get_num_partition() do
    [{_, _, num}] = Ets.lookup(Holder, :num_partition)
    num
  end

  @doc "Return the number of online schedulers (used as default partition count)."
  @spec get_schedulers() :: pos_integer
  def get_schedulers(), do: System.schedulers_online()

  # kept for backwards compat — prefer get_partition_order/1
  @doc false
  @spec get_hash(any) :: non_neg_integer
  def get_hash(term), do: :erlang.phash2(term)

  # kept for backwards compat — prefer get_partition_order/1
  @doc false
  @spec get_pattition_order(any) :: non_neg_integer
  def get_pattition_order(term), do: get_partition_order(term)
end
