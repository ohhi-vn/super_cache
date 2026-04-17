defmodule SuperCache.KeyValue do
  @moduledoc """
  In-memory key-value namespaces backed by SuperCache ETS partitions.

  Works transparently in both **local** and **distributed** modes — the
  mode is determined by the `:cluster` option passed to `SuperCache.start!/1`.

  Multiple independent namespaces coexist using different `kv_name` values.

  ## ETS table type support

  KeyValue adapts its behavior based on the configured `:table_type`:

  | Operation       | `:set` / `:ordered_set`              | `:bag` / `:duplicate_bag`          |
  |-----------------|--------------------------------------|-------------------------------------|
  | `add/3`         | Atomic upsert (update or insert)     | Insert (duplicates allowed)        |
  | `get/3`         | Single value or default             | Most recent value or default       |
  | `get_all/3`     | List with at most one element        | All values for the key             |
  | `update/3`      | Atomic via `update_element`          | Delete-all + insert (not atomic)   |
  | `update/4`      | Best-effort read-modify-write        | Best-effort read-modify-write       |
  | `increment/4`  | Atomic via `update_counter`          | Not supported (raises)             |
  | `replace/3`     | Same as `update/3`                   | Delete-all + insert                |

  ## Atomic operations

  - `update/3` — atomically set a value using `:ets.update_element/3,4`
    (`:set`/`:ordered_set` only). If the key does not exist, a new record
    is inserted (upsert semantics).
  - `increment/4` — atomically increment a counter using
    `:ets.update_counter/3,4` (`:set`/`:ordered_set` only).
  - `add/3` on `:set`/`:ordered_set` tables uses `update_element` with a
    default for true atomic upsert semantics in `:async`/`:sync` replication
    mode.

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

      # Atomic operations (:set / :ordered_set tables)
      KeyValue.update("counters", :hits, 1)           # => :ok
      KeyValue.increment("counters", :hits, 0, 1)     # => 2

      # Bag table support
      KeyValue.get_all("tags", :elixir)               # => [1, 2]  (all values)
      KeyValue.replace("tags", :elixir, 3)            # => :ok     (replace all)
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.{Storage, Partition, Config}
  alias SuperCache.Cluster.{Manager, Replicator, ThreePhaseCommit, Router}

  ## ── Public API ──────────────────────────────────────────────────────────────

  @doc """
  Add or update a key-value pair.

  For `:set`/`:ordered_set` tables, this is an atomic upsert — if the key
  exists, the value is updated in-place via `:ets.update_element`; if not,
  a new record is inserted.

  For `:bag`/`:duplicate_bag` tables, this inserts a new record. Duplicate
  keys are allowed. Use `replace/3` to atomically replace all values for a key.

  Returns `true`.
  """
  @spec add(any, any, any) :: true
  def add(kv_name, key, value) do
    SuperCache.Log.debug(fn -> "super_cache, kv #{inspect(kv_name)}, add key=#{inspect(key)}" end)

    if distributed?() do
      route_write(kv_name, :local_put, [kv_name, key, value])
    else
      local_put_local(kv_name, key, value)
    end
  end

  @doc """
  Get the value for `key`, returning `default` if not found.

  For `:set`/`:ordered_set` tables, returns the single value or `default`.

  For `:bag`/`:duplicate_bag` tables, returns the **most recently inserted**
  value. Use `get_all/3` to retrieve all values for a key.
  """
  @spec get(any, any, any, keyword) :: any
  def get(kv_name, key, default \\ nil, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_get, [kv_name, key, default], opts)
    else
      local_get(kv_name, key, default)
    end
  end

  @doc """
  Get all values for `key` as a list.

  Useful for `:bag`/`:duplicate_bag` tables where multiple records can share
  the same key. For `:set`/`:ordered_set` tables, returns a list with at most
  one element.
  """
  @spec get_all(any, any, keyword) :: [any]
  def get_all(kv_name, key, opts \\ []) do
    if distributed?() do
      route_read(kv_name, :local_get_all, [kv_name, key], opts)
    else
      local_get_all(kv_name, key)
    end
  end

  @doc """
  Atomically set the value for `key` (upsert semantics).

  For `:set`/`:ordered_set` tables, uses `:ets.update_element/3,4` which is
  guaranteed atomic at the ETS level. If the key doesn't exist, a new record
  is inserted.

  For `:bag`/`:duplicate_bag` tables, deletes all existing records for the key
  and inserts a new one. This is **not atomic** — a concurrent reader may
  observe the key as missing between the delete and the insert. Prefer
  `:set` or `:ordered_set` tables when atomic updates are required.

  Returns `:ok`.
  """
  @spec update(any, any, any) :: :ok
  def update(kv_name, key, value) do
    SuperCache.Log.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, update key=#{inspect(key)}"
    end)

    if distributed?() do
      route_write(kv_name, :local_update, [kv_name, key, value])
    else
      local_update_local(kv_name, key, value)
    end
  end

  @doc """
  Update the value for `key` using a function.

  `fun` receives the current value (or `default` if the key doesn't exist)
  and must return the new value.

  **Warning**: This is a read-modify-write operation and is **not atomic**.
  A concurrent writer may modify the value between the read and the write,
  causing a lost update. For atomic value updates, use `update/3`. For
  atomic counter increments, use `increment/4`.

  In distributed mode, `fun` is serialized and sent to the primary node via
  `:erpc`. The function must not capture node-specific resources (PIDs, etc.).

  Returns the new value.
  """
  @spec update(any, any, any, (any -> any)) :: any
  def update(kv_name, key, default, fun) when is_function(fun, 1) do
    if distributed?() do
      route_write(kv_name, :local_update_fun, [kv_name, key, default, fun])
    else
      local_update_fun_local(kv_name, key, default, fun)
    end
  end

  @doc """
  Atomically increment a counter field.

  The value at `key` must be a number. If the key doesn't exist, `default`
  is used as the initial value before incrementing by `step`.

  Only supported for `:set`/`:ordered_set` tables. Raises `ArgumentError`
  for `:bag`/`:duplicate_bag` tables — use `:set` or `:ordered_set` for
  atomic counter operations.

  Returns the new counter value.
  """
  @spec increment(any, any, number, number) :: number
  def increment(kv_name, key, default \\ 0, step \\ 1) do
    if distributed?() do
      route_write(kv_name, :local_increment, [kv_name, key, default, step])
    else
      local_increment_local(kv_name, key, default, step)
    end
  end

  @doc """
  Replace all values for `key` with a single value.

  For `:bag`/`:duplicate_bag` tables, this deletes all existing records for
  the key and inserts a single new record. For `:set`/`:ordered_set` tables,
  this is equivalent to `update/3` (atomic upsert).

  **Note**: For `:bag`/`:duplicate_bag` tables, this operation is not atomic —
  a concurrent reader may observe the key as missing between the delete and
  the insert.

  Returns `:ok`.
  """
  @spec replace(any, any, any) :: :ok
  def replace(kv_name, key, value) do
    SuperCache.Log.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, replace key=#{inspect(key)}"
    end)

    if distributed?() do
      route_write(kv_name, :local_replace, [kv_name, key, value])
    else
      local_replace_local(kv_name, key, value)
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
    SuperCache.Log.debug(fn ->
      "super_cache, kv #{inspect(kv_name)}, remove key=#{inspect(key)}"
    end)

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
    records =
      Enum.map(pairs, fn {key, value} ->
        {{:kv, kv_name, key}, value}
      end)

    if distributed?() do
      SuperCache.put_batch!(records)
    else
      partition = Partition.get_partition(kv_name)
      Storage.put(records, partition)
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
      Enum.each(keys, fn key ->
        ets_key = {:kv, kv_name, key}
        Router.route_delete_by_key_partition!(ets_key, kv_name)
      end)
    else
      partition = Partition.get_partition(kv_name)

      Enum.each(keys, fn key ->
        :ets.delete(partition, {:kv, kv_name, key})
      end)
    end

    :ok
  end

  ## ── Remote entry points — writes (called via :erpc on primary) ──────────────

  @doc false
  def local_put(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        # Atomic upsert: update_element with default inserts if missing,
        # updates in-place if present. Position 2 is the value field
        # in the tuple {{:kv, kv_name, key}, value}.
        case Manager.replication_mode() do
          :strong ->
            apply_write(idx(kv_name), partition, [{:put, {ets_key, value}}])

          _ ->
            Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})
            Replicator.replicate(idx(kv_name), :put, {ets_key, value})
        end

      _ ->
        # :bag / :duplicate_bag — insert adds a new record (duplicates ok)
        apply_write(idx(kv_name), partition, [{:put, {ets_key, value}}])
    end

    true
  end

  @doc false
  def local_delete(kv_name, key) do
    apply_write(idx(kv_name), Partition.get_partition(kv_name), [{:delete, {:kv, kv_name, key}}])
    :ok
  end

  @doc false
  def local_delete_all(kv_name) do
    apply_write(idx(kv_name), Partition.get_partition(kv_name), [
      {:delete_match, {{:kv, kv_name, :_}, :_}}
    ])

    :ok
  end

  @doc false
  def local_update(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        case Manager.replication_mode() do
          :strong ->
            apply_write(idx(kv_name), partition, [{:put, {ets_key, value}}])

          _ ->
            Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})
            Replicator.replicate(idx(kv_name), :put, {ets_key, value})
        end

      _ ->
        # :bag / :duplicate_bag — delete all + insert (not atomic)
        apply_write(idx(kv_name), partition, [
          {:delete, ets_key},
          {:put, {ets_key, value}}
        ])
    end

    :ok
  end

  @doc false
  def local_update_fun(kv_name, key, default, fun) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    current =
      case Storage.get(ets_key, partition) do
        [] -> default
        records -> extract_value(records, default)
      end

    new_value = fun.(current)

    case table_type() do
      t when t in [:set, :ordered_set] ->
        case Manager.replication_mode() do
          :strong ->
            apply_write(idx(kv_name), partition, [{:put, {ets_key, new_value}}])

          _ ->
            Storage.update_element(ets_key, partition, {2, new_value}, {ets_key, new_value})
            Replicator.replicate(idx(kv_name), :put, {ets_key, new_value})
        end

      _ ->
        apply_write(idx(kv_name), partition, [
          {:delete, ets_key},
          {:put, {ets_key, new_value}}
        ])
    end

    new_value
  end

  @doc false
  def local_increment(kv_name, key, default, step) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        case Manager.replication_mode() do
          :strong ->
            # For strong consistency, use read-modify-write via 3PC.
            # This has a TOCTOU race but is consistent with 3PC semantics.
            current =
              case Storage.get(ets_key, partition) do
                [] -> default
                [{_, v}] -> v
              end

            new_value = current + step
            apply_write(idx(kv_name), partition, [{:put, {ets_key, new_value}}])
            new_value

          _ ->
            # Atomic counter increment — update_counter is an atomic ETS op.
            new_value = Storage.update_counter(ets_key, partition, {2, step}, {ets_key, default})
            Replicator.replicate(idx(kv_name), :put, {ets_key, new_value})
            new_value
        end

      _ ->
        raise ArgumentError, """
        KeyValue.increment/4 is not supported for :bag/:duplicate_bag tables.
        Use :set or :ordered_set table type for atomic counter operations.
        """
    end
  end

  @doc false
  def local_replace(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        # Same as local_update for set tables (atomic upsert)
        case Manager.replication_mode() do
          :strong ->
            apply_write(idx(kv_name), partition, [{:put, {ets_key, value}}])

          _ ->
            Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})
            Replicator.replicate(idx(kv_name), :put, {ets_key, value})
        end

      _ ->
        # :bag / :duplicate_bag — delete all records for key, then insert new one
        apply_write(idx(kv_name), partition, [
          {:delete, ets_key},
          {:put, {ets_key, value}}
        ])
    end

    :ok
  end

  ## ── Remote entry points — reads (called via :erpc in quorum/primary reads) ──

  @doc false
  def local_get(kv_name, key, default) do
    case Storage.get({:kv, kv_name, key}, Partition.get_partition(kv_name)) do
      [] -> default
      records -> extract_value(records, default)
    end
  end

  @doc false
  def local_get_all(kv_name, key) do
    Storage.get({:kv, kv_name, key}, Partition.get_partition(kv_name))
    |> Enum.map(fn {_, value} -> value end)
  end

  @doc false
  def local_keys(kv_name) do
    do_match(kv_name) |> Enum.map(fn {{:kv, ^kv_name, k}, _} -> k end) |> Enum.uniq()
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
  defp idx(name), do: Partition.get_partition_order(name)
  defp table_type(), do: Config.get_config(:table_type, :set)

  # Extract a single value from a list of ETS records.
  # For :set/:ordered_set tables, there is at most one record.
  # For :bag/:duplicate_bag tables, take the last (most recent) value.
  defp extract_value([], default), do: default

  defp extract_value(records, _default) do
    {_, value} = List.last(records)
    value
  end

  defp do_match(kv_name) do
    Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, Partition.get_partition(kv_name))
  end

  # Local-mode only: atomic upsert using update_element with default.
  defp local_put_local(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})

      _ ->
        Storage.put({ets_key, value}, partition)
    end

    true
  end

  # Local-mode only: atomic update using update_element with default.
  defp local_update_local(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})

      _ ->
        Storage.delete(ets_key, partition)
        Storage.put({ets_key, value}, partition)
    end

    :ok
  end

  # Local-mode only: read-modify-write with update_element.
  defp local_update_fun_local(kv_name, key, default, fun) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    current =
      case Storage.get(ets_key, partition) do
        [] -> default
        records -> extract_value(records, default)
      end

    new_value = fun.(current)

    case table_type() do
      t when t in [:set, :ordered_set] ->
        Storage.update_element(ets_key, partition, {2, new_value}, {ets_key, new_value})

      _ ->
        Storage.delete(ets_key, partition)
        Storage.put({ets_key, new_value}, partition)
    end

    new_value
  end

  # Local-mode only: atomic counter increment.
  defp local_increment_local(kv_name, key, default, step) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        Storage.update_counter(ets_key, partition, {2, step}, {ets_key, default})

      _ ->
        raise ArgumentError, """
        KeyValue.increment/4 is not supported for :bag/:duplicate_bag tables.
        Use :set or :ordered_set table type for atomic counter operations.
        """
    end
  end

  # Local-mode only: replace all values for a key.
  defp local_replace_local(kv_name, key, value) do
    partition = Partition.get_partition(kv_name)
    ets_key = {:kv, kv_name, key}

    case table_type() do
      t when t in [:set, :ordered_set] ->
        Storage.update_element(ets_key, partition, {2, value}, {ets_key, value})

      _ ->
        Storage.delete(ets_key, partition)
        Storage.put({ets_key, value}, partition)
    end

    :ok
  end

  defp apply_write(idx, partition, ops) do
    case Manager.replication_mode() do
      :strong ->
        ThreePhaseCommit.commit(idx, ops)

      _ ->
        Enum.each(ops, fn
          {:put, r} ->
            Storage.put(r, partition)
            Replicator.replicate(idx, :put, r)

          {:delete, k} ->
            Storage.delete(k, partition)
            Replicator.replicate(idx, :delete, k)

          {:delete_match, p} ->
            Storage.delete_match(p, partition)
            Replicator.replicate(idx, :delete_match, p)

          {:delete_all, _} ->
            Storage.delete_all(partition)
            Replicator.replicate(idx, :delete_all, nil)
        end)

        :ok
    end
  end

  defp route_write(kv_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(kv_name))

    if primary == node() do
      apply(__MODULE__, fun, args)
    else
      SuperCache.Log.debug(fn ->
        "super_cache, kv #{inspect(kv_name)}, fwd #{fun} → #{inspect(primary)}"
      end)

      :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp route_read(kv_name, fun, args, opts) do
    mode = Keyword.get(opts, :read_mode, :local)
    eff = if mode == :local and not has_partition?(kv_name), do: :primary, else: mode

    case eff do
      :local -> apply(__MODULE__, fun, args)
      :primary -> read_from_primary(kv_name, fun, args)
      :quorum -> read_from_quorum(kv_name, fun, args)
    end
  end

  defp has_partition?(name) do
    {p, rs} = Manager.get_replicas(idx(name))
    node() in [p | rs]
  end

  defp read_from_primary(kv_name, fun, args) do
    {primary, _} = Manager.get_replicas(idx(kv_name))

    if primary == node(),
      do: apply(__MODULE__, fun, args),
      else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
  end

  defp read_from_quorum(kv_name, fun, args) do
    {primary, replicas} = Manager.get_replicas(idx(kv_name))
    nodes = [primary | replicas]
    total = length(nodes)
    required = div(total, 2) + 1

    # Launch all reads as independent tasks for early termination.
    tasks =
      Enum.map(nodes, fn n ->
        Task.async(fn ->
          if n == node(),
            do: apply(__MODULE__, fun, args),
            else: :erpc.call(n, __MODULE__, fun, args, 5_000)
        end)
      end)

    # Await tasks one-by-one, returning as soon as any result reaches majority.
    # This avoids waiting for slow replicas once quorum is satisfied.
    await_quorum(tasks, required, %{}, primary, fun, args)
  end

  # No tasks left — return most frequent result or fall back to primary.
  defp await_quorum([], _required, counts, primary, fun, args) do
    case map_max(counts) do
      {result, _count} ->
        result

      nil ->
        if primary == node(),
          do: apply(__MODULE__, fun, args),
          else: :erpc.call(primary, __MODULE__, fun, args, 5_000)
    end
  end

  defp await_quorum([task | rest], required, counts, primary, fun, args) do
    result =
      case Task.yield(task, 5_000) || Task.shutdown(task, :brutal_kill) do
        {:ok, val} -> val
        _ -> nil
      end

    new_counts =
      if result != nil do
        Map.update(counts, result, 1, &(&1 + 1))
      else
        counts
      end

    # Check if any result has reached quorum — early termination.
    if quorum_reached?(new_counts, required) do
      Enum.each(rest, &Task.shutdown(&1, :brutal_kill))
      {winner, _} = Enum.max_by(new_counts, fn {_, c} -> c end)
      winner
    else
      await_quorum(rest, required, new_counts, primary, fun, args)
    end
  end

  # Returns {result, count} for the most frequent result, or nil if map is empty.
  defp map_max(counts) when map_size(counts) == 0, do: nil
  defp map_max(counts), do: Enum.max_by(counts, fn {_, c} -> c end)

  # True if any result has reached the required quorum count.
  defp quorum_reached?(counts, required) do
    Enum.any?(counts, fn {_, count} -> count >= required end)
  end
end
