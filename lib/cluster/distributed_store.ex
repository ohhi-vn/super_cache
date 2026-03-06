defmodule SuperCache.Cluster.DistributedStore do
  @moduledoc """
  Shared routing helpers used by all distributed high-level stores
  (KeyValue, Queue, Stack, Struct).

  ## Write routing
  Every write wraps its data in a tuple whose partition value is derived
  from the store's namespace key (kv_name, queue_name, stack_name, or
  struct_name).  The tuple is handed to `Router.route_put!/1` which
  forwards it to the correct primary node if needed.

  ## Read routing
  Reads always go to the local ETS table (eventual consistency by default).
  Pass `read_mode: :primary` where strong consistency is required.

  ## Delete routing
  Single-key deletes use `Router.route_delete_by_key_partition!/2`.
  Pattern deletes use `Router.route_delete_match!/2`.
  """

  alias SuperCache.Cluster.Router
  alias SuperCache.{Storage, Partition}

  ## Write helpers ##

  @doc """
  Route a put for a record whose ETS key is `ets_key` and whose partition
  is determined by `namespace` (the store name).

  Builds the canonical two-element tuple `{ets_key, value}` and routes it.
  The partition position is 0 (the key), so `ets_key` must embed the
  namespace so that partition hashing is namespace-scoped.
  """
  @spec route_put(any, any) :: true
  def route_put(ets_key, value) do
    Router.route_put!({ets_key, value})
  end

  @doc "Route a delete for `ets_key` whose partition is derived from `namespace`."
  @spec route_delete(any, any) :: :ok
  def route_delete(ets_key, namespace) do
    Router.route_delete_by_key_partition!(ets_key, namespace)
  end

  @doc "Route a pattern delete scoped to `namespace`."
  @spec route_delete_match(any, tuple) :: :ok
  def route_delete_match(namespace, pattern) do
    Router.route_delete_match!(namespace, pattern)
  end

  ## Read helpers (always local) ##

  @doc "Read `ets_key` from the local partition for `namespace`."
  @spec local_get(any, any) :: [tuple]
  def local_get(ets_key, namespace) do
    partition = Partition.get_partition(namespace)
    Storage.get(ets_key, partition)
  end

  @doc "Match-object scan on the local partition for `namespace`."
  @spec local_match(any, tuple) :: [tuple]
  def local_match(namespace, pattern) do
    partition = Partition.get_partition(namespace)
    Storage.get_by_match_object(pattern, partition)
  end

  @doc "insert_new on the local partition for `namespace`."
  @spec local_insert_new(tuple, any) :: boolean
  def local_insert_new(record, namespace) do
    partition = Partition.get_partition(namespace)
    Storage.insert_new(record, partition)
  end

  @doc "take on the local partition for `namespace`."
  @spec local_take(any, any) :: [tuple]
  def local_take(ets_key, namespace) do
    partition = Partition.get_partition(namespace)
    Storage.take(ets_key, partition)
  end
end
