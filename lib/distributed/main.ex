defmodule SuperCache.Distributed do
  @moduledoc """
  Cluster-aware public API for SuperCache.

  This module is the distributed drop-in replacement for the single-node
  `SuperCache` API.  Every write is routed to the partition's **primary node**
  automatically; reads default to the local ETS table for lowest latency.

  ## Routing

  Partition ownership is determined by `SuperCache.Cluster.Manager`, which
  hashes each partition value across the sorted list of live nodes.  When the
  caller's node is the primary, the write is applied locally and then
  replicated.  When it is not, the write is forwarded via `:erpc` to the
  primary, which then applies and replicates it.

  ```
  Any node              Primary node         Replica nodes
     |-- put!({…}) ------> local_put          apply_op (async)
     |                          |-----------> replica 1
     |                          |-----------> replica 2
  ```

  See `SuperCache.Cluster.Router` for the full forwarding logic and the
  design rule that prevents forwarding cycles.

  ## Read modes

  | Mode       | Consistency               | Latency              |
  |------------|---------------------------|----------------------|
  | `:local`   | Eventual                  | Zero extra latency   |
  | `:primary` | Strong (per key)          | +1 RTT if non-primary|
  | `:quorum`  | Majority vote             | +1 RTT (parallel)   |

  `:local` is the default.  Use `:primary` or `:quorum` when you need to
  read your own writes from any node in the cluster.

  ## Replication modes

  Configured via `:replication_mode` in `Bootstrap.start!/1`:

  | Mode      | Guarantee              | Extra latency    |
  |-----------|------------------------|------------------|
  | `:async`  | Eventual (default)     | None             |
  | `:sync`   | At-least-once delivery | +1 RTT per write |
  | `:strong` | Three-phase commit     | +3 RTTs per write|

  ## Setup

  ```elixir
      SuperCache.Cluster.Bootstrap.start!(
        key_pos:            0,
        partition_pos:      0,
        cluster:            :distributed,
        replication_factor: 2,
        replication_mode:   :async,
        num_partition:      16
      )
  ```

  ## Basic usage

  ```elixir
      alias SuperCache.Distributed, as: Cache

      # Write — routed to the correct primary automatically
      Cache.put!({:user, 1, "Alice", :admin})

      # Eventual read from local replica (fastest)
      Cache.get!({:user, 1, nil, nil})

      # Strong read — forwarded to primary if this node is not the primary
      Cache.get!({:user, 1, nil, nil}, read_mode: :primary)

      # Quorum read — majority of replicas must agree
      Cache.get!({:user, 1, nil, nil}, read_mode: :quorum)

      # Delete
      Cache.delete!({:user, 1, nil, nil})

      # Pattern delete across all partitions
      Cache.delete_by_match!(:_, {:user, :_, :_, :guest})

      # Full cluster wipe (one routed call per partition)
      Cache.delete_all()
  ```

  ## Error handling

  The `!`-suffix functions raise on error.  The non-bang variants return
  `{:error, exception}` instead:

  ```elixir
      case Cache.put(record) do
        true            -> :ok
        {:error, reason} -> :failed
      end
  ```
  """

  require Logger

  alias SuperCache.Cluster.Router

  ## Write ──────────────────────────────────────────────────────────────────────

  @doc """
  Store a tuple.  Routes to the partition's primary node if needed.

  Raises on error.  See module docs for routing behaviour.

  ## Example

      Cache.put!({:order, "ord-1", :eu, :pending, 99.00})
  """
  @spec put!(tuple) :: true
  def put!(data) when is_tuple(data), do: Router.route_put!(data)

  @doc """
  Store a tuple.

  Returns `true | {:error, exception}` instead of raising.
  """
  @spec put(tuple) :: true | {:error, Exception.t()}
  def put(data), do: safe(fn -> Router.route_put!(data) end)

  ## Read ───────────────────────────────────────────────────────────────────────

  @doc """
  Retrieve records matching the key in `data`.

  ## Options

  - `:read_mode` — `:local` (default), `:primary`, or `:quorum`.

  ## Examples

      # Default: read from local ETS (may be stale on a replica)
      Cache.get!({:user, 1, nil})

      # Strong: always read from the primary
      Cache.get!({:user, 1, nil}, read_mode: :primary)

      # Quorum: at least ⌈(replicas+1)/2⌉ nodes must return the same value
      Cache.get!({:user, 1, nil}, read_mode: :quorum)
  """
  @spec get!(tuple, keyword) :: [tuple]
  def get!(data, opts \\ []) when is_tuple(data), do: Router.route_get!(data, opts)

  @doc """
  Retrieve records.

  Returns `[tuple] | {:error, exception}` instead of raising.
  """
  @spec get(tuple, keyword) :: [tuple] | {:error, Exception.t()}
  def get(data, opts \\ []), do: safe(fn -> Router.route_get!(data, opts) end)

  ## Delete ─────────────────────────────────────────────────────────────────────

  @doc """
  Delete the record matching `data`.  Routes to the primary for this key.

  ## Example

      Cache.delete!({:session, "tok-xyz", nil})
  """
  @spec delete!(tuple) :: :ok
  def delete!(data) when is_tuple(data), do: Router.route_delete!(data)

  @doc """
  Delete all records across every partition on every node.

  Each partition delete is routed to its own primary, so this issues
  `num_partition` forwarded calls concurrently.
  """
  @spec delete_all() :: :ok
  def delete_all(), do: Router.route_delete_all()

  @doc """
  Delete records matching `pattern` in `partition_data` (or all partitions
  when `:_`).  Routes to the correct primary per partition.

  ## Examples

      # Remove all expired sessions from the :eu partition
      Cache.delete_by_match!(:eu, {:session, :_, :expired})

      # Wipe a record shape cluster-wide
      Cache.delete_by_match!(:_, {:tmp_lock, :_, :_})
  """
  @spec delete_by_match!(any, tuple) :: :ok
  def delete_by_match!(partition_data, pattern) when is_tuple(pattern) do
    Router.route_delete_match!(partition_data, pattern)
  end

  @doc """
  Delete by explicit key and partition value.  Routes to the primary.

  ## Example

      Cache.delete_by_key_partition!("tok-abc", :eu)
  """
  @spec delete_by_key_partition!(any, any) :: :ok
  def delete_by_key_partition!(key, partition_data) do
    Router.route_delete_by_key_partition!(key, partition_data)
  end

  ## Private ────────────────────────────────────────────────────────────────────

  defp safe(fun) do
    fun.()
  rescue
    err ->
      Logger.error(Exception.format(:error, err, __STACKTRACE__))
      {:error, err}
  end
end
