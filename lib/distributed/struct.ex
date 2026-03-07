defmodule SuperCache.Distributed.Struct do
  @moduledoc """
  Cluster-aware struct store.

  Writes (`add`, `remove`, `remove_all`, `init`) are routed to the primary
  node for the partition that owns the struct type.  Reads (`get`, `get_all`)
  default to the local node but can be forwarded to the primary or resolved
  via quorum when stronger consistency is needed.

  The API is identical to `SuperCache.Struct` — swap the alias to migrate:

      # Single-node
      alias SuperCache.Struct, as: S

      # Cluster-aware
      alias SuperCache.Distributed.Struct, as: S

  ## Partition strategy

  Every operation hashes `{:struct_storage, StructName}` to find the
  partition.  This namespace key is consistent across puts, deletes, and
  reads, so all operations for a struct type always hit the same ETS table
  regardless of which node handles the request.

  ## Read modes

  | Mode       | Consistency             | Latency               |
  |------------|-------------------------|-----------------------|
  | `:local`   | Eventual (default)      | Zero extra latency    |
  | `:primary` | Strong (per key)        | +1 RTT if non-primary |
  | `:quorum`  | Majority vote           | +1 RTT (parallel)     |

  Use `:primary` or `:quorum` when a replica may be stale and you need to
  read your own writes from any node.

  ## Example

      alias SuperCache.Distributed.Struct, as: DS

      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0, partition_pos: 0,
        cluster: :distributed, replication_factor: 2
      )

      defmodule Order do
        defstruct [:id, :customer, :total, :status]
      end

      DS.init(%Order{}, :id)

      DS.add(%Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending})
      DS.add(%Order{id: "o-2", customer: "Bob",   total: 12.00, status: :shipped})

      # Eventual read from local replica (fastest)
      DS.get(%Order{id: "o-1"})

      # Strong read — always hits the primary
      DS.get(%Order{id: "o-1"}, read_mode: :primary)

      # Quorum read — majority of replicas must agree
      DS.get(%Order{id: "o-1"}, read_mode: :quorum)

      {:ok, all} = DS.get_all(%Order{})
      length(all)   # => 2

      DS.remove(%Order{id: "o-1"})
      DS.remove_all(%Order{})
  """

  alias SuperCache.{Storage, Partition}
  alias SuperCache.Cluster.{Manager, Replicator}
  require Logger

  ## Public API ─────────────────────────────────────────────────────────────────

  @doc """
  Register `key` as the lookup field for the struct type of `struct`.

  Routed to the primary node.  Must be called before any other operation on
  this struct type.

  ## Example

      DS.init(%Order{}, :id)    # => true
      DS.init(%Order{}, :id)    # => {:error, "struct already initialised"}
  """
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

  @doc """
  Store `struct`.  Routed to the primary node.

  Overwrites any existing struct with the same key.

  ## Example

      DS.add(%Order{id: "o-1", customer: "Alice", total: 59.99, status: :pending})
      # => {:ok, %Order{...}}
  """
  @spec add(map) :: {:ok, map} | {:error, any}
  def add(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      route_write(struct, :local_add, [struct])
    end
  end

  @doc """
  Retrieve a struct by its key field.

  Defaults to reading from the **local node** (`:local` mode). Pass
  `read_mode: :primary` to always hit the partition's primary, or
  `read_mode: :quorum` to require a majority of replicas to agree.

  Returns `{:ok, struct}` or `{:error, :not_found}`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      DS.get(%Order{id: "o-1"})
      # => {:ok, %Order{...}}

      DS.get(%Order{id: "o-1"}, read_mode: :primary)
      # => {:ok, %Order{...}}   (forwarded to primary if needed)

      DS.get(%Order{id: "nope"})
      # => {:error, :not_found}
  """
  @spec get(map, keyword) :: {:ok, map} | {:error, :not_found | any}
  def get(%{__struct__: _} = struct, opts \\ []) do
    read_mode = Keyword.get(opts, :read_mode, :local)

    with {:ok, _key} <- get_key_field(struct) do
      route_read(struct, :local_get, [struct], read_mode)
    end
  end

  @doc """
  Return all structs of this type.

  Defaults to reading from the **local node** (`:local` mode). Supports the
  same `:read_mode` options as `get/2`.

  Returns `{:ok, [struct]}`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Example

      {:ok, orders} = DS.get_all(%Order{})
      {:ok, orders} = DS.get_all(%Order{}, read_mode: :primary)
      Enum.map(orders, & &1.status)
  """
  @spec get_all(map, keyword) :: {:ok, list} | {:error, any}
  def get_all(%{__struct__: _} = struct, opts \\ []) do
    read_mode = Keyword.get(opts, :read_mode, :local)

    with {:ok, _key} <- get_key_field(struct) do
      route_read(struct, :local_get_all, [struct], read_mode)
    end
  end

  @doc """
  Remove the struct matching the key field of `struct`.  Routed to the primary.

  Returns `{:ok, removed_struct}` or `{:error, :not_found}`.

  ## Example

      DS.remove(%Order{id: "o-1"})   # => {:ok, %Order{...}}
      DS.remove(%Order{id: "o-1"})   # => {:error, :not_found}
  """
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

  @doc """
  Remove all structs of this type.  Routed to the primary.

  The key field registration is preserved; you can call `add/1` again
  without calling `init/2`.

  ## Example

      DS.remove_all(%Order{})    # => {:ok, :removed}
      DS.get_all(%Order{})       # => {:ok, []}
  """
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

  ## Remote entry points (called via :erpc — do NOT call directly) ──────────────

  @doc false
  def local_init(%{__struct__: struct_name} = struct, key) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)
    idx = Partition.get_partition_order(ns)
    record = {{:struct_storage, :key, struct_name}, key}
    Storage.put(record, partition)
    Replicator.replicate(idx, :put, record)
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

      Storage.delete(ets_key, partition)
      Storage.put({ets_key, struct}, partition)
      Replicator.replicate(idx, :delete, ets_key)
      Replicator.replicate(idx, :put, {ets_key, struct})
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
      Storage.delete(ets_key, partition)
      Replicator.replicate(idx, :delete, ets_key)
      :ok
    end
  end

  @doc false
  def local_remove_all(%{__struct__: struct_name} = struct) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)
    idx = Partition.get_partition_order(ns)
    pattern = {{{:struct_storage, :struct, struct_name}, :_}, :_}
    Storage.delete_match(pattern, partition)
    Replicator.replicate(idx, :delete_match, pattern)
    {:ok, :removed}
  end

  # Read entry points called via :erpc for :primary / :quorum routing.
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

  ## Private ────────────────────────────────────────────────────────────────────

  # Consistent namespace key for ALL partition hashing in this module.
  defp namespace(%{__struct__: struct_name}), do: {:struct_storage, struct_name}

  defp get_key_field(%{__struct__: struct_name} = struct) do
    ns = namespace(struct)
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      [{_, key}] ->
        {:ok, key}

      [] ->
        # Local ETS missed — this node may not hold the partition at all.
        # Fall back to the primary, which is guaranteed to have it.
        fetch_key_field_from_primary(struct, struct_name)
    end
  end

  defp fetch_key_field_from_primary(struct, struct_name) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      # We *are* the primary yet still got a miss — the key was never init'd.
      {:error, :key_not_found}
    else
      Logger.debug(fn ->
        "super_cache, dist.struct, get_key_field fallback → #{inspect(primary)}"
      end)

      case :erpc.call(primary, __MODULE__, :local_get_key_field, [struct_name], 5_000) do
        {:ok, _key} = ok -> ok
        other -> other
      end
    end
  end

  # Called via :erpc from fetch_key_field_from_primary/2 on remote nodes.
  @doc false
  def local_get_key_field(struct_name) do
    # Build a minimal struct just to resolve the namespace / partition.
    ns = {:struct_storage, struct_name}
    partition = Partition.get_partition(ns)

    case Storage.get({:struct_storage, :key, struct_name}, partition) do
      [{_, key}] -> {:ok, key}
      [] -> {:error, :key_not_found}
    end
  end

  # ── Write routing ──────────────────────────────────────────────────────────

  defp route_write(struct, fun, args) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, _replicas} = Manager.get_replicas(idx)

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      Logger.debug(fn ->
        "super_cache, dist.struct, fwd #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  # ── Read routing ───────────────────────────────────────────────────────────

  # :local — read directly from this node's ETS (original behaviour).
  defp route_read(struct, fun, args, :local) do
    apply(__MODULE__, fun, args)
  end

  # :primary — forward to the primary if this node is not the primary.
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

  # :quorum — ask every replica in parallel; return the majority result.
  # Falls back to the primary's answer on a tie.
  defp route_read(struct, fun, args, :quorum) do
    idx = Partition.get_partition_order(namespace(struct))
    {primary, replicas} = Manager.get_replicas(idx)
    all_nodes = [primary | replicas]

    results =
      all_nodes
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
        # timed-out or crashed node → skip
        _ -> []
      end)

    quorum_result(results, fn ->
      # Tie-break: ask the primary directly.
      if primary == node() do
        apply(__MODULE__, fun, args)
      else
        :erpc.call(primary, __MODULE__, fun, args, 5_000)
      end
    end)
  end

  # Choose the result that appears in the strict majority of responses.
  # Calls `tiebreak_fn.()` when no majority exists.
  defp quorum_result(results, tiebreak_fn) do
    majority = div(length(results), 2) + 1

    winner =
      results
      |> Enum.frequencies()
      |> Enum.find(fn {_result, count} -> count >= majority end)

    case winner do
      {result, _count} -> result
      nil -> tiebreak_fn.()
    end
  end
end
