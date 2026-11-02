defmodule SuperCache.Struct do
  @moduledoc """
  In-memory struct store backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  Call `init/2` once per struct type before using `add/1`, `get/1`, etc.

  The initialized key field is cached in `:persistent_term` after `init/2`
  resolves it, so repeated operations avoid an ETS lookup (and any remote
  fallback). The cache is cleared when the cache is stopped.

  ## Read modes (distributed)

  Pass `read_mode: :primary` or `read_mode: :quorum` for stronger consistency.

  ## Example

      alias SuperCache.Struct, as: S

      defmodule Order do
        defstruct [:id, :customer, :status]
      end

      S.init(%Order{}, :id)
      S.add(%Order{id: "o-1", customer: "Alice", status: :pending})
      S.get(%Order{id: "o-1"})
      # => {:ok, %Order{id: "o-1", customer: "Alice", status: :pending}}
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Cluster.{DistributedHelpers, Manager}
  alias SuperCache.{Config, Partition, Storage}

  # Cache resolved struct key fields in persistent_term so repeated operations
  # do not need an ETS lookup (or an :erpc fallback to the primary).
  @pt_key_field {__MODULE__, :key_field}

  # ── Public API ───────────────────────────────────────────────────────────────

  @spec init(map, atom) :: true | {:error, any}
  def init(%{__struct__: _} = struct, key \\ :id) when is_atom(key) do
    with true <- Map.has_key?(struct, key),
         {:error, :key_not_found} <- get_key_field(struct) do
      if Config.distributed?() do
        DistributedHelpers.route_write(
          __MODULE__,
          :local_init,
          [struct, key],
          Partition.get_partition_order(ns(struct))
        )
      else
        do_local_init(struct, key)
      end
    else
      false -> {:error, "key does not exist on struct"}
      {:ok, _} -> {:error, "struct already initialised"}
    end
  end

  @spec add(map) :: {:ok, map} | {:error, any}
  def add(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      if Config.distributed?() do
        DistributedHelpers.route_write(
          __MODULE__,
          :local_add,
          [struct],
          Partition.get_partition_order(ns(struct))
        )
      else
        do_local_add(struct)
      end
    end
  end

  @spec get(map, keyword) :: {:ok, map} | {:error, :not_found | any}
  def get(%{__struct__: _} = struct, opts \\ []) do
    with {:ok, _key} <- get_key_field(struct) do
      if Config.distributed?() do
        distributed_read(:local_get, struct, opts)
      else
        do_local_get(struct)
      end
    end
  end

  @spec get_all(map, keyword) :: {:ok, list} | {:error, any}
  def get_all(%{__struct__: _} = struct, opts \\ []) do
    with {:ok, _key} <- get_key_field(struct) do
      if Config.distributed?() do
        distributed_read(:local_get_all, struct, opts)
      else
        do_local_get_all(struct)
      end
    end
  end

  defp distributed_read(operation, struct, opts) do
    DistributedHelpers.read_or_error(fn ->
      DistributedHelpers.route_read(
        __MODULE__,
        operation,
        [struct],
        Partition.get_partition_order(ns(struct)),
        opts
      )
    end)
  end

  @doc """
  Remove a struct by its initialized key.

  In distributed mode the lookup and deletion execute atomically on the
  partition primary, so a lagging local replica cannot cause a false
  `:not_found`. Returns `{:ok, removed_struct}` or `{:error, :not_found}`.
  """
  @spec remove(map) :: {:ok, map} | {:error, any}
  def remove(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      if Config.distributed?() do
        partition_idx = Partition.get_partition_order(ns(struct))
        DistributedHelpers.route_write(__MODULE__, :local_remove, [struct], partition_idx)
      else
        do_local_remove(struct)
      end
    end
  end

  @spec remove_all(map) :: {:ok, :removed} | {:error, any}
  def remove_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      SuperCache.Log.debug(fn -> "super_cache, struct, remove_all #{inspect(struct_name)}" end)

      if Config.distributed?() do
        do_remote_remove_all(struct)
      else
        do_local_remove_all(struct)
      end
    end
  end

  defp do_remote_remove_all(struct) do
    partition_idx = Partition.get_partition_order(ns(struct))

    case DistributedHelpers.route_write(__MODULE__, :local_remove_all, [struct], partition_idx) do
      :ok -> {:ok, :removed}
      {:ok, :removed} -> {:ok, :removed}
      other -> other
    end
  end

  # ── Remote entry points — writes (called via :erpc on primary) ───────────────
  #
  # All `local_*` functions are PUBLIC so they are reachable across the cluster.
  # They MUST NOT share a name (or arity) with any private `defp` in this module.
  # The private local-mode helpers are prefixed `do_local_*` to avoid collision.

  @doc false
  def local_init(%{__struct__: struct_name} = struct, key) do
    ns = ns(struct)
    idx = Partition.get_partition_order(ns)

    case DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [
           {:put, {{:struct_storage, :key, struct_name}, key}}
         ]) do
      :ok ->
        cache_key_field(struct_name, key)
        true

      {:error, _} = error ->
        error
    end
  end

  @doc false
  def local_add(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = ns(struct)
      idx = Partition.get_partition_order(ns)
      key_data = Map.get(struct, key)
      ets_key = {{:struct_storage, :struct, struct_name}, key_data}

      case DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [
             {:delete, ets_key},
             {:put, {ets_key, struct}}
           ]) do
        :ok -> {:ok, struct}
        {:error, _} = error -> error
      end
    end
  end

  @doc false
  def local_remove(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = ns(struct)
      idx = Partition.get_partition_order(ns)
      partition = Partition.get_partition(ns)
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}

      case Storage.get(ets_key, partition) do
        [] -> {:error, :not_found}
        [{_, existing}] -> delete_existing(ets_key, existing, idx, partition)
      end
    end
  end

  defp delete_existing(ets_key, existing, idx, partition) do
    case DistributedHelpers.apply_write(idx, partition, [{:delete, ets_key}]) do
      :ok -> {:ok, existing}
      {:error, _} = error -> error
    end
  end

  @doc false
  def local_remove_all(%{__struct__: struct_name} = struct) do
    ns = ns(struct)
    idx = Partition.get_partition_order(ns)
    pattern = {{{:struct_storage, :struct, struct_name}, :_}, :_}

    case DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [
           {:delete_match, pattern}
         ]) do
      :ok -> {:ok, :removed}
      {:error, _} = error -> error
    end
  end

  # ── Remote entry points — reads ───────────────────────────────────────────────

  @doc false
  def local_get(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}

      case Storage.get(ets_key, Partition.get_partition(ns(struct))) do
        [] -> {:error, :not_found}
        [{_, result}] -> {:ok, result}
      end
    end
  end

  @doc false
  def local_get_all(%{__struct__: struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      results =
        Storage.get_by_match_object(
          {{{:struct_storage, :struct, struct_name}, :_}, :_},
          Partition.get_partition(ns(struct))
        )
        |> Enum.map(fn {_, v} -> v end)

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

  # ── Private — local-mode implementations (prefixed do_local_ to avoid clash) ──

  defp do_local_init(%{__struct__: struct_name} = struct, key) do
    Storage.put(
      {{:struct_storage, :key, struct_name}, key},
      Partition.get_partition(ns(struct))
    )

    cache_key_field(struct_name, key)
    true
  end

  defp do_local_add(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      key_data = Map.get(struct, key)
      ets_key = {{:struct_storage, :struct, struct_name}, key_data}
      part = Partition.get_partition(ns(struct))
      Storage.delete(ets_key, part)
      Storage.put({ets_key, struct}, part)
      {:ok, struct}
    end
  end

  defp do_local_remove(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}
      part = Partition.get_partition(ns(struct))

      case Storage.get(ets_key, part) do
        [] ->
          {:error, :not_found}

        [{_, result}] ->
          Storage.delete(ets_key, part)
          {:ok, result}
      end
    end
  end

  defp do_local_remove_all(%{__struct__: struct_name} = struct) do
    Storage.delete_match(
      {{{:struct_storage, :struct, struct_name}, :_}, :_},
      Partition.get_partition(ns(struct))
    )

    {:ok, :removed}
  end

  defp do_local_get(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}
      partition = Partition.get_partition(ns(struct))

      case Storage.get(ets_key, partition) do
        [] -> {:error, :not_found}
        [{_, result}] -> {:ok, result}
      end
    end
  end

  defp do_local_get_all(%{__struct__: struct_name} = struct) do
    results =
      Storage.get_by_match_object(
        {{{:struct_storage, :struct, struct_name}, :_}, :_},
        Partition.get_partition(ns(struct))
      )
      |> Enum.map(fn {_, v} -> v end)

    {:ok, results}
  end

  # ── Private — shared helpers ─────────────────────────────────────────────────

  defp ns(%{__struct__: struct_name}), do: {:struct_storage, struct_name}

  @doc false
  @spec clear_key_field_cache() :: :ok
  def clear_key_field_cache do
    for {{@pt_key_field, _struct_name} = key, _value} <- :persistent_term.get() do
      :persistent_term.erase(key)
    end

    :ok
  end

  defp cache_key_field(struct_name, key) do
    :persistent_term.put({@pt_key_field, struct_name}, key)
    :ok
  end

  defp get_key_field(%{__struct__: struct_name} = struct) do
    case :persistent_term.get({@pt_key_field, struct_name}, :not_cached) do
      :not_cached -> lookup_key_field(struct, struct_name)
      key -> {:ok, key}
    end
  end

  defp lookup_key_field(struct, struct_name) do
    case Storage.get({:struct_storage, :key, struct_name}, Partition.get_partition(ns(struct))) do
      [{_, key}] ->
        cache_key_field(struct_name, key)
        {:ok, key}

      [] ->
        fetch_key_field_from_primary(struct, struct_name)
    end
  end

  defp fetch_key_field_from_primary(struct, struct_name) do
    {primary, _} = Manager.get_replicas(Partition.get_partition_order(ns(struct)))

    if primary == node() do
      {:error, :key_not_found}
    else
      case :erpc.call(primary, __MODULE__, :local_get_key_field, [struct_name], 5_000) do
        {:ok, key} = result ->
          cache_key_field(struct_name, key)
          result

        other ->
          other
      end
    end
  end
end
