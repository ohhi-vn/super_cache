defmodule SuperCache.Cluster.DistributedStore do
  @moduledoc """
  Shared routing helpers used by distributed high-level stores.

  ## Partition resolution

  Two consistent schemes are provided — pick one per access pattern:

  - **Concrete-key helpers** (`route_put/2`, `route_delete/2`, `local_get/2`,
    `local_insert_new/2`, `local_take/2`) resolve the partition from the
    record or record key itself, so writes, reads and deletes for the same
    key always land on the same table. The second argument is accepted for
    API compatibility and ignored.
  - **Pattern helpers** (`local_match/2`, `route_delete_match/2`) cannot hash
    a match pattern (wildcards change the term), so they scope to a
    namespace: the partition is derived from the namespace argument and all
    records for one store must therefore embed that namespace in their key.

  ## Read routing

  Reads always go to the local ETS table (eventual consistency by default).
  Pass `read_mode: :primary` at the API layer where strong consistency is
  required.
  """

  alias SuperCache.Cluster.Router
  alias SuperCache.{Storage, Partition}

  ## Write helpers ##

  @doc """
  Route a put of the record `{ets_key, value}` to the partition derived
  from `ets_key`.
  """
  @spec route_put(any, any) :: true
  def route_put(ets_key, value) do
    Router.route_put!({ets_key, value})
  end

  @doc "Route a delete of `ets_key`, partitioned by the key itself."
  @spec route_delete(any, any) :: :ok
  def route_delete(ets_key, _namespace) do
    Router.route_delete_by_key_partition!(ets_key, ets_key)
  end

  @doc "Route a pattern delete scoped to the partition derived from `namespace`."
  @spec route_delete_match(any, tuple) :: :ok
  def route_delete_match(namespace, pattern) do
    Router.route_delete_match!(namespace, pattern)
  end

  ## Read helpers (always local) ##

  @doc "Read `ets_key` from its partition."
  @spec local_get(any, any) :: [tuple]
  def local_get(ets_key, _namespace) do
    partition = Partition.get_partition(ets_key)
    Storage.get(ets_key, partition)
  end

  @doc "Match-object scan on the namespace's partition."
  @spec local_match(any, tuple) :: [tuple]
  def local_match(namespace, pattern) do
    partition = Partition.get_partition(namespace)
    Storage.get_by_match_object(pattern, partition)
  end

  @doc "insert_new on the record's partition."
  @spec local_insert_new(tuple, any) :: boolean
  def local_insert_new(record, _namespace) do
    partition = Partition.get_partition(record)
    Storage.insert_new(record, partition)
  end

  @doc "take on the record's partition."
  @spec local_take(any, any) :: [tuple]
  def local_take(ets_key, _namespace) do
    partition = Partition.get_partition(ets_key)
    Storage.take(ets_key, partition)
  end
end
