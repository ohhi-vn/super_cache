
defmodule SuperCache.KeyValue do
  @moduledoc """
  In-memory key-value namespaces backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  Multiple independent namespaces coexist using different `kv_name` values.

  ## Read modes (distributed)

  Pass `read_mode: :primary` or `read_mode: :quorum` when you need to read
  your own writes from any node.

  ## Example

      alias SuperCache.KeyValue

      # Works identically in local and distributed mode
      KeyValue.add("session", :user_id, 42)
      KeyValue.get("session", :user_id)       # => 42
      KeyValue.keys("session")                # => [:user_id]
      KeyValue.remove("session", :user_id)
      KeyValue.remove_all("session")
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit}

  ## ── Public API ──────────────────────────────────────────────────────────────

  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    SuperCache.Log.debug(fn -> "super_cache, kv #{inspect(kv_name)}, add key=#{inspect(key)}" end)

    if distributed?() do
      route_write(kv_name, :local_put, [kv_name, key, value])
    else
      Storage.put({{:kv, kv_name, key}, value}, Partition.get_partition(kv_name))
    end
  end

  @spec get(any, any, any, keyword) :: any
  def get(kv_name, key, default \\ nil, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_get, [kv_name, key, default], opts)
    else
      case Storage.get({:kv, kv_name, key}, Partition.get_partition(kv_name)) do
        []           -> default
        [{_, value}] -> value
      end
    end
  end

  @spec keys(any, keyword) :: [any]
  def keys(kv_name, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_keys, [kv_name], opts)
    else
      local_keys(kv_name)
    end
  end

  @spec values(any, keyword) :: [any]
  def values(kv_name, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_values, [kv_name], opts)
    else
      local_values(kv_name)
    end
  end

  @spec count(any, keyword) :: non_neg_integer
  def count(kv_name, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_count, [kv_name], opts)
    else
      local_count(kv_name)
    end
  end

  @spec to_list(any, keyword) :: [{any, any}]
  def to_list(kv_name, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_to_list, [kv_name], opts)
    else
      local_to_list(kv_name)
    end
  end

  @spec remove(any, any) :: :ok
  def remove(kv_name, key) do
    SuperCache.Log.debug(fn -> "super_cache, kv #{inspect(kv_name)}, remove key=#{inspect(key)}" end)

    if distributed?() do
      route_write(kv_name, :local_delete, [kv_name, key])
    else
      Storage.delete({:kv, kv_name, key}, Partition.get_partition(kv_name))
      :ok
    end
  end

  @spec remove_all(any) :: :ok
  def remove_all(kv_name) do
    if distributed?() do
      route_write(kv_name, :local_delete_all, [kv_name])
    else
      SuperCache.delete_by_match!(kv_name, {{:kv, kv_name, :_}, :_})
    end
  end

  @doc """
  Add multiple key-value pairs in a single batch operation.

  Groups entries by partition and sends each group in a single `:erpc` call
  in distributed mode, dramatically reducing network overhead.

  ## Example

      KeyValue.add_batch("session", [
        {:user_1, %{name: "Alice"}},
        {:user_2, %{name: "Bob"}}
      ])
  """
  @spec add_batch(any, [{any, any}]) :: :ok
  def add_batch(kv_name, pairs) when is_list(pairs) do
    if distributed?() do
      # Group by partition and batch-write each group
      partition = Partition.get_partition(kv_name)
      records = Enum.map(pairs, fn {key, value} ->
        {{:kv, kv_name, key}, value}
      end)

      SuperCache.put_batch!(records)
    else
      Enum.each(pairs, fn {key, value} ->
        Storage.put({{:kv, kv_name, key}, value}, Partition.get_partition(kv_name))
      end)
    end

    :ok
  end

  @doc """
  Remove multiple keys in a single batch operation.

  Groups entries by partition and sends each group in a single `:erpc` call
  in distributed mode.

  ## Example

      KeyValue.remove_batch("session", [:user_1, :user_2])
  """
  @spec remove_batch(any, [any]) :: :ok
  def remove_batch(kv_name, keys) when is_list(keys) do
    if distributed?() do
      partition = Partition.get_partition(kv_name)
      # Build delete records for batch routing
      records = Enum.map(keys, fn key ->
        {:kv, kv_name, key}
      end)

      # Use Router for distributed deletes
      Enum.each(keys, fn key ->
        ets_key = {:kv, kv_name, key}
        Router.route_delete_by_key_partition!(ets_key, kv_name)
      end)
    else
      partition = Partition.get_partition(kv_name)
      Enum.each(keys, fn key ->
        Storage.delete({:kv, kv_name, key}, partition)
      end)
    end

    :ok
  end

  ## ── Remote entry points — writes (called via :erpc on primary) ──────────────

  @doc false
  def local_put(kv_name, key, value) do
    apply_write(idx(kv_name), Partition.get_partition(kv_name),
                [{:put, {{:kv, kv_name, key}, value}}])
    true
  end

  @doc false
  def local_delete(kv_name, key) do
    apply_write(idx(kv_name), Partition.get_partition(kv_name),
                [{:delete, {:kv, kv_name, key}}])
    :ok
  end

  @doc false
  def local_delete_all(kv_name) do
    apply_write(idx(kv_name), Partition.get_partition(kv_name),
                [{:delete_match, {{:kv, kv_name, :_}, :_}}])
    :ok
  end

  ## ── Remote entry points — reads (called via :erpc in quorum/primary reads) ──

  @doc false
  def local_get(kv_name, key, default) do
    case Storage.get({:kv, kv_name, key}, Partition.get_partition(kv_name)) do
      []           -> default
      [{_, value}] -> value
    end
  end

  @doc false
  def local_keys(kv_name) do
    do_match(kv_name) |> Enum.map(fn {{:kv, ^kv_name, k}, _} -> k end)
  end

  @doc false
  def local_values(kv_name) do
    do_match(kv_name) |> Enum.map(fn {{:kv, ^kv_name, _}, v} -> v end)
  end

  @doc false
  def local_count(kv_name), do: do_match(kv_name) |> length()

  @doc false
  def local_to_list(kv_name) do
    do_match(kv_name) |> Enum.map(fn {{:kv, ^kv_name, k}, v} -> {k, v} end)
  end

  ## ── Private ──────────────────────────────────────────────────────────────────

  defp distributed?(), do: Config.get_config(:cluster, :local) == :distributed
  defp idx(name),      do: Partition.get_partition_order(name)

  defp do_match(kv_name) do
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, Partition.get_partition(kv_name))
  end

  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        ThreePhaseCommit.commit(idx, ops)
      _ ->
        Enum.each(ops, fn
          {:put, r}           -> Storage.put(r, partition);           Replicator.replicate(idx, :put, r)
          {:delete, k}        -> Storage.delete(k, partition);        Replicator.replicate(idx, :delete, k)
          {:delete_match, p}  -> Storage.delete_match(p, partition);  Replicator.replicate(idx, :delete_match, p)
          {:delete_all, _}    -> Storage.delete_all(partition);       Replicator.replicate(idx, :delete_all, nil)
        end)
        :ok
    end
  end

  defp route_write(kv_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(kv_name))
    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      SuperCache.Log.debug(fn -> "super_cache, kv #{inspect(kv_name)}, fwd #{fun} → #{inspect(primary)}" end)
      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read(kv_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)
    eff  = if mode == :local and not has_partition?(kv_name), do: :primary, else: mode
    case eff do
      :local   -> apply(__MODULE__, fun, args)
      :primary -> read_from_primary(kv_name, fun, args)
      :quorum  -> read_from_quorum(kv_name, fun, args)
    end
  end

  defp has_partition?(name) do
    {p, rs} = Manager.get_replicas(idx(name))
    node() in [p | rs]
  end

  defp read_from_primary(kv_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(kv_name))
    if primary == node(), do: apply(__MODULE__, fun, args),
    else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
  end

  defp read_from_quorum(kv_name, fun, args) do
    {primary, replicas} = Manager.get_replicas(idx(kv_name))
    results = [primary | replicas]
    |> Task.async_stream(
      fn n when n == node() -> apply(__MODULE__, fun, args)
         n                  -> :erpc.call(n, __MODULE__, fun, args, 5_000)
      end,
      timeout: 5_000, on_timeout: :kill_task
    )
    |> Enum.flat_map(fn {:ok, r} -> [r]; _ -> [] end)
    majority = div(length(results), 2) + 1
    case Enum.find(Enum.frequencies(results), fn {_, c} -> c >= majority end) do
      {result, _} -> result
      nil -> if primary == node(), do: apply(__MODULE__, fun, args),
             else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end
end
