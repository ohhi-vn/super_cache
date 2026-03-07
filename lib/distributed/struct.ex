defmodule SuperCache.Distributed.Struct do
  @moduledoc """
  Cluster-aware struct store.

  Writes (`add`, `remove`, `remove_all`, `init`) are routed to the primary
  node for the partition that owns the struct type.  Reads (`get`, `get_all`)
  default to the local node but can be forwarded to the primary or resolved
  via quorum when stronger consistency is needed.

  Write replication mode is controlled by the cluster-wide `:replication_mode`
  setting (see `SuperCache.Cluster.Bootstrap`):

  | Mode      | Guarantee              | Extra latency    |
  |-----------|------------------------|------------------|
  | `:async`  | Eventual (default)     | None             |
  | `:sync`   | At-least-once delivery | +1 RTT per write |
  | `:strong` | Three-phase commit     | +3 RTTs per write|

  ## Read modes

  | Mode       | Consistency             | Latency               |
  |------------|-------------------------|-----------------------|
  | `:local`   | Eventual (default)      | Zero extra latency    |
  | `:primary` | Strong (per key)        | +1 RTT if non-primary |
  | `:quorum`  | Majority vote           | +1 RTT (parallel)     |

  ## Example

      alias SuperCache.Distributed.Struct, as: DS

      DS.init(%Order{}, :id)
      DS.add(%Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending})

      DS.get(%Order{id: "o-1"})
      DS.get(%Order{id: "o-1"}, read_mode: :primary)
      DS.get(%Order{id: "o-1"}, read_mode: :quorum)

      {:ok, all} = DS.get_all(%Order{})
      DS.remove(%Order{id: "o-1"})
      DS.remove_all(%Order{})
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}
  require Logger

  ## ── Public API ──────────────────────────────────────────────────────────────

  @spec init(map, atom) :: true | {:error, any}
  def init(%{__struct__: _} = struct, key \\ :id) when is_atom(key) do
    with true <- Map.has_key?(struct, key),
         {:error, :key_not_found} <- get_key_field(struct) do
      route_write(struct, :local_init, [struct, key])
    else
      false -> {:error, "key does not exist on struct"}
      {:ok, _} -> {:error, "struct already initialised"}
    end
  end

  @spec add(map) :: {:ok, map} | {:error, any}
  def add(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      route_write(struct, :local_add, [struct])
    end
  end

  @spec get(map, keyword) :: {:ok, map} | {:error, :not_found | any}
  def get(%{__struct__: _} = struct, opts \\ []) do
    with {:ok, _key} <- get_key_field(struct) do
      route_read(struct, :local_get, [struct], Keyword.get(opts, :read_mode, :local))
    end
  end

  @spec get_all(map, keyword) :: {:ok, list} | {:error, any}
  def get_all(%{__struct__: _} = struct, opts \\ []) do
    with {:ok, _key} <- get_key_field(struct) do
      route_read(struct, :local_get_all, [struct], Keyword.get(opts, :read_mode, :local))
    end
  end

  @spec remove(map) :: {:ok, map} | {:error, any}
  def remove(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      case get(struct) do
        {:error, :not_found} = err ->
          err

        {:ok, _existing} = return ->
          case route_write(struct, :local_remove, [struct]) do
            :ok -> return
            {:ok, _} -> return
            other -> other
          end
      end
    end
  end

  @spec remove_all(map) :: {:ok, :removed} | {:error, any}
  def remove_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      Logger.debug(fn -> "super_cache, dist.struct, remove_all #{inspect(struct_name)}" end)

      case route_write(struct, :local_remove_all, [struct]) do
        :ok -> {:ok, :removed}
        {:ok, _} -> {:ok, :removed}
        other -> other
      end
    end
  end

  ## ── Remote entry points — writes (via :erpc, do NOT call directly) ──────────

  @doc false
  def local_init(%{__struct__: struct_name} = struct, key) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)
    idx = Partition.get_partition_order(ns)
    apply_write(idx, partition, [{:put, {{:struct_storage, :key, struct_name}, key}}])
    true
  end

  @doc false
  def local_add(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = namespace(struct)
      partition = Partition.get_partition(ns)
      idx = Partition.get_partition_order(ns)
      key_data = Map.get(struct, key)
      ets_key = {{:struct_storage, :struct, struct_name}, key_data}

      Logger.debug(fn ->
        "super_cache, dist.struct, add #{inspect(struct_name)} key=#{inspect(key_data)}"
      end)

      apply_write(idx, partition, [{:delete, ets_key}, {:put, {ets_key, struct}}])
      {:ok, struct}
    end
  end

  @doc false
  def local_remove(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = namespace(struct)
      partition = Partition.get_partition(ns)
      idx = Partition.get_partition_order(ns)
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}
      apply_write(idx, partition, [{:delete, ets_key}])
      :ok
    end
  end

  @doc false
  def local_remove_all(%{__struct__: struct_name} = struct) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)
    idx = Partition.get_partition_order(ns)
    pattern = {{{:struct_storage, :struct, struct_name}, :_}, :_}
    apply_write(idx, partition, [{:delete_match, pattern}])
    {:ok, :removed}
  end

  ## ── Remote entry points — reads (via :erpc, do NOT call directly) ───────────

  @doc false
  def local_get(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = namespace(struct)
      partition = Partition.get_partition(ns)
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}

      case Storage.get(ets_key, partition) do
        [] -> {:error, :not_found}
        [{_, result}] -> {:ok, result}
      end
    end
  end

  @doc false
  def local_get_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      ns = namespace(struct)
      partition = Partition.get_partition(ns)

      results =
        Storage.get_by_match_object(
          {{{:struct_storage, :struct, struct_name}, :_}, :_},
          partition
        )
        |> Enum.map(fn {_, value} -> value end)

      {:ok, results}
    end
  end

  @doc false
  def local_get_key_field(struct_name) do
    ns = {:struct_storage, struct_name}
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      [{_, key}] -> {:ok, key}
      [] -> {:error, :key_not_found}
    end
  end

  ## ── Private ──────────────────────────────────────────────────────────────────

  defp namespace(%{__struct__: struct_name}), do: {:struct_storage, struct_name}

  defp get_key_field(%{__struct__: struct_name} = struct) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      [{_, key}] ->
        {:ok, key}

      [] ->
        # Not found locally — node may not hold this partition. Fall back to primary.
        fetch_key_field_from_primary(struct, struct_name)
    end
  end

  defp fetch_key_field_from_primary(struct, struct_name) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      # We are the primary yet got a miss — struct was never init'd.
      {:error, :key_not_found}
    else
      Logger.debug(fn ->
        "super_cache, dist.struct, get_key_field fallback → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, :local_get_key_field, [struct_name], 5_000)
    end
  end

  # ── Write routing ─────────────────────────────────────────────────────────────

  defp route_write(struct, fun, args) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn -> "super_cache, dist.struct, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # In :strong mode ThreePhaseCommit.commit/2 handles both local Storage apply
  # and replica propagation atomically (primary applies last after replicas ACK).
  # In other modes Storage is written locally first then replicated async.
  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        case ThreePhaseCommit.commit(idx, ops) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("super_cache, dist.struct, 3pc failed: #{inspect(reason)}")
            {:error, reason}
        end

      _ ->
        Enum.each(ops, fn
          {:put, record} ->
            Storage.put(record, partition)
            Replicator.replicate(idx, :put, record)

          {:delete, key} ->
            Storage.delete(key, partition)
            Replicator.replicate(idx, :delete, key)

          {:delete_match, pattern} ->
            Storage.delete_match(pattern, partition)
            Replicator.replicate(idx, :delete_match, pattern)

          {:delete_all, _} ->
            Storage.delete_all(partition)
            Replicator.replicate(idx, :delete_all, nil)
        end)

        :ok
    end
  end

  # ── Read routing ──────────────────────────────────────────────────────────────

  defp route_read(_struct, fun, args, :local) do
    apply(__MODULE__, fun, args)
  end

  defp route_read(struct, fun, args, :primary) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.struct, read_primary #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read(struct, fun, args, :quorum) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, replicas} = Manager.get_replicas(idx)

    results =
      [primary | replicas]
      |> Task.async_stream(
        fn
          n when n == node() -> apply(__MODULE__, fun, args)
          n -> :erpc.call(n, __MODULE__, fun, args, 5_000)
        end,
        timeout: 5_000,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, result} -> [result]
        _ -> []
      end)

    majority = div(length(results), 2) + 1

    case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
      {result, _} ->
        result

      nil ->
        if primary == node(),
          do: apply(__MODULE__, fun, args),
          else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
