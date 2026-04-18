defmodule SuperCache.Struct do
  @moduledoc """
  In-memory struct store backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  Call `init/2` once per struct type before using `add/1`, `get/1`, etc.

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

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, DistributedHelpers}

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
        DistributedHelpers.route_read(
          __MODULE__,
          :local_get,
          [struct],
          Partition.get_partition_order(ns(struct)),
          opts
        )
      else
        do_local_get(struct)
      end
    end
  end

  @spec get_all(map, keyword) :: {:ok, list} | {:error, any}
  def get_all(%{__struct__: _} = struct, opts \\ []) do
    with {:ok, _key} <- get_key_field(struct) do
      if Config.distributed?() do
        DistributedHelpers.route_read(
          __MODULE__,
          :local_get_all,
          [struct],
          Partition.get_partition_order(ns(struct)),
          opts
        )
      else
        do_local_get_all(struct)
      end
    end
  end

  @spec remove(map) :: {:ok, map} | {:error, any}
  def remove(%{__struct__: _} = struct) do
    with {:ok, _key} <- get_key_field(struct),
         {:ok, _existing} <- get(struct) do
      if Config.distributed?() do
        partition_idx = Partition.get_partition_order(ns(struct))

        case DistributedHelpers.route_write(__MODULE__, :local_remove, [struct], partition_idx) do
          :ok -> {:ok, struct}
          {:ok, _} -> {:ok, struct}
          other -> other
        end
      else
        do_local_remove(struct)
      end
    end
  end

  @spec remove_all(map) :: {:ok, :removed} | {:error, any}
  def remove_all(%{__struct__: _struct_name} = struct) do
    with {:ok, _key} <- get_key_field(struct) do
      SuperCache.Log.debug(fn -> "super_cache, struct, remove_all #{inspect(_struct_name)}" end)

      if Config.distributed?() do
        partition_idx = Partition.get_partition_order(ns(struct))

        case DistributedHelpers.route_write(
               __MODULE__,
               :local_remove_all,
               [struct],
               partition_idx
             ) do
          :ok -> {:ok, :removed}
          {:ok, :removed} -> {:ok, :removed}
          other -> other
        end
      else
        do_local_remove_all(struct)
      end
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

    DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [
      {:put, {{:struct_storage, :key, struct_name}, key}}
    ])

    true
  end

  @doc false
  def local_add(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = ns(struct)
      idx = Partition.get_partition_order(ns)
      key_data = Map.get(struct, key)
      ets_key = {{:struct_storage, :struct, struct_name}, key_data}

      DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [
        {:delete, ets_key},
        {:put, {ets_key, struct}}
      ])

      {:ok, struct}
    end
  end

  @doc false
  def local_remove(%{__struct__: struct_name} = struct) do
    with {:ok, key} <- get_key_field(struct) do
      ns = ns(struct)
      idx = Partition.get_partition_order(ns)
      ets_key = {{:struct_storage, :struct, struct_name}, Map.get(struct, key)}
      DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [{:delete, ets_key}])
      :ok
    end
  end

  @doc false
  def local_remove_all(%{__struct__: struct_name} = struct) do
    ns = ns(struct)
    idx = Partition.get_partition_order(ns)
    pattern = {{{:struct_storage, :struct, struct_name}, :_}, :_}
    DistributedHelpers.apply_write(idx, Partition.get_partition(ns), [{:delete_match, pattern}])
    {:ok, :removed}
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

  defp get_key_field(%{__struct__: struct_name} = struct) do
    case Storage.get({:struct_storage, :key, struct_name}, Partition.get_partition(ns(struct))) do
      [{_, key}] -> {:ok, key}
      [] -> fetch_key_field_from_primary(struct, struct_name)
    end
  end

  defp fetch_key_field_from_primary(struct, struct_name) do
    {primary, _} = Manager.get_replicas(Partition.get_partition_order(ns(struct)))

    if primary == node() do
      {:error, :key_not_found}
    else
      :erpc.call(primary, __MODULE__, :local_get_key_field, [struct_name], 5_000)
    end
  end
end
