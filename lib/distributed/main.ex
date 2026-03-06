defmodule SuperCache.Distributed do
  @moduledoc """
  Drop-in replacements for `SuperCache.put!/1`, `get!/1`, and the delete
  family that are cluster-aware.

  Import or alias this module when running in cluster mode instead of
  calling `SuperCache` directly, or update `SuperCache` to delegate here
  when `:cluster` config is `:distributed`.
  """
  require Logger

  alias SuperCache.{Config, Partition, Storage}
  alias SuperCache.Cluster.{Manager, Replicator}

  ## Write ##

  @doc "Store a tuple on the local node and replicate to peer replicas."
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data) do
    partition_data = Config.get_partition!(data)
    partition_idx  = Partition.get_pattition_order(partition_data)
    partition      = Partition.get_partition(partition_data)

    result = Storage.put(data, partition)
    Replicator.replicate(partition_idx, :put, data)
    result
  end

  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> put!(data) end)

  @doc """
  Retrieve records by key.

  Options:
    - `read_mode: :local` (default) — read from local ETS immediately.
    - `read_mode: :primary` — if this node is not the primary, forward the
      read to the primary node for strong consistency.
  """
  @spec get!(tuple, keyword) :: [tuple]
  def get!(data, opts \\ []) when is_tuple(data) do
    key            = Config.get_key!(data)
    partition_data = Config.get_partition!(data)
    partition_idx  = Partition.get_pattition_order(partition_data)
    partition      = Partition.get_partition(partition_data)

    case Keyword.get(opts, :read_mode, :local) do
      :local ->
        Storage.get(key, partition)

      :primary ->
        {primary, _} = Manager.get_replicas(partition_idx)

        if primary == node() do
          Storage.get(key, partition)
        else
          :erpc.call(primary, Storage, :get, [key, partition], 5_000)
        end
    end
  end

  @spec get(tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get(data, opts \\ []), do: safe(fn -> get!(data, opts) end)

  ## Delete ##

  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data) do
    key            = Config.get_key!(data)
    partition_data = Config.get_partition!(data)
    partition_idx  = Partition.get_pattition_order(partition_data)
    partition      = Partition.get_partition(partition_data)

    Storage.delete(key, partition)
    Replicator.replicate(partition_idx, :delete, key)
    :ok
  end

  @spec delete_all() :: :ok
  def delete_all() do
    Partition.get_all_partition()
    |> List.flatten()
    |> Enum.with_index()
    |> Enum.each(fn {p, idx} ->
      Storage.delete_all(p)
      Replicator.replicate(idx, :delete_all)
    end)
  end

  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    partitions =
      case partition_data do
        :_ -> List.flatten(Partition.get_all_partition()) |> Enum.with_index()
        d  -> [{Partition.get_partition(d), Partition.get_pattition_order(d)}]
      end

    Enum.each(partitions, fn {p, idx} ->
      Storage.delete_match(pattern, p)
      Replicator.replicate(idx, :delete_match, pattern)
    end)
  end

  ## Private ##

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end

## ─────────────────────────────────────────────────────────────
## SuperCache.Distributed  (now delegates entirely to Router)
## ─────────────────────────────────────────────────────────────
defmodule SuperCache.Distributed do
  @moduledoc """
  Cluster-aware public API for SuperCache.

  Every write is routed to the partition's primary node automatically.
  Reads default to local ETS for lowest latency; pass `read_mode: :primary`
  or `read_mode: :quorum` when consistency matters.

  ## Read modes
  | Mode       | Latency  | Consistency         |
  |------------|----------|---------------------|
  | `:local`   | ~0 μs    | Eventual            |
  | `:primary` | 1 hop    | Strong (linearizable per key) |
  | `:quorum`  | 1 hop    | Majority vote       |
  """

  require Logger

  alias SuperCache.Cluster.Router

  ## Write ##

  @doc "Store a tuple. Routes to primary if needed. Raises on error."
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data), do: Router.route_put!(data)

  @doc "Store a tuple. Returns `true | {:error, reason}`."
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> Router.route_put!(data) end)

  ## Read ##

  @doc """
  Retrieve records matching the key in `data`.

      # Eventual read (default, fastest)
      Distributed.get!({:user, 1, nil})

      # Strong read — forwarded to primary if needed
      Distributed.get!({:user, 1, nil}, read_mode: :primary)

      # Quorum read — majority of replicas must agree
      Distributed.get!({:user, 1, nil}, read_mode: :quorum)
  """
  @spec get!(tuple, keyword) :: [tuple]
  def get!(data, opts \\ []) when is_tuple(data), do: Router.route_get!(data, opts)

  @doc "Retrieve records. Returns `[tuple] | {:error, reason}`."
  @spec get(tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get(data, opts \\ []), do: safe(fn -> Router.route_get!(data, opts) end)

  ## Delete ##

  @doc "Delete the record whose key/partition match `data`. Routes to primary."
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data), do: Router.route_delete!(data)

  @doc "Delete all records across all partitions (routed per-partition)."
  @spec delete_all() :: :ok
  def delete_all(), do: Router.route_delete_all()

  @doc "Delete records matching `pattern` in a partition (or all with `:_`)."
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    Router.route_delete_match!(partition_data, pattern)
  end

  @doc "Delete by explicit key and partition value."
  @spec delete_by_key_partition!(any, any) :: :ok
  def delete_by_key_partition!(key, partition_data) do
    Router.route_delete_by_key_partition!(key, partition_data)
  end

  ## Private ##

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
