# SuperCache - Distributed Mode Example
#
# This example demonstrates how to use SuperCache in distributed (cluster) mode
# with three nodes. It showcases replication modes, read modes, and fault tolerance.
#
# Run with: mix run examples/distributed_mode_example.exs
#
# NOTE: This example uses :peer to spawn child nodes within the same BEAM instance
# for demonstration purposes. In production, you would start separate BEAM nodes
# with proper distribution flags (--name, --cookie).

alias SuperCache.{KeyValue, Queue, Stack}
alias SuperCache.Cluster.Manager

# ── 1. Cluster Setup ──────────────────────────────────────────────────────────

IO.puts("=== Starting SuperCache (Distributed Mode - 3 Nodes) ===\n")

# Base configuration for all nodes (MUST be identical across cluster)
cluster_opts = [
  key_pos: 0,
  partition_pos: 0,
  cluster: :distributed,
  replication_factor: 2,
  replication_mode: :async,
  num_partition: 8,
  table_type: :set
]

# Start the primary node (current node)
IO.puts("Starting primary node: #{inspect(node())}")
SuperCache.start!(cluster_opts)
IO.puts("Primary node started successfully!")
IO.puts("Distributed mode? #{SuperCache.distributed?()}")
IO.puts("")

# ── 2. Simulate Peer Nodes ────────────────────────────────────────────────────
# In production, these would be separate BEAM instances started with:
#   elixir --name node1@10.0.0.1 --cookie secret -S mix
#   elixir --name node2@10.0.0.2 --cookie secret -S mix

IO.puts("=== Peer Node Simulation ===\n")
IO.puts("NOTE: In production, start separate BEAM nodes with --name and --cookie flags.")
IO.puts("This example demonstrates the API concepts using the primary node.\n")

# For demonstration, we'll show how to configure peer discovery
IO.puts("To connect to peers, configure in config/runtime.exs:")

IO.puts("""
  config :super_cache,
    cluster_peers: [
      :"node1@10.0.0.1",
      :"node2@10.0.0.2",
      :"node3@10.0.0.3"
    ]
""")

# Show cluster status
IO.puts("Current cluster status:")
IO.puts("  Live nodes: #{inspect(Manager.live_nodes())}")
IO.puts("  Replication mode: #{inspect(Manager.replication_mode())}")
IO.puts("  Replication factor: 2 (configured)")
IO.puts("")

# ── 3. Replication Modes ──────────────────────────────────────────────────────

IO.puts("=== Replication Modes ===\n")

IO.puts("""
SuperCache supports three replication modes:

1. :async (Eventual Consistency)
   - Fire-and-forget replication via worker pool
   - Latency: ~50-100µs
   - Use case: High-throughput caches, session data
   - Risk: Data loss if primary crashes before replication

2. :sync (Adaptive Quorum)
   - Returns :ok once majority of replicas acknowledge
   - Latency: ~100-300µs
   - Use case: Balanced durability/performance
   - Tolerates slow/failed replicas

3. :strong (WAL-based Strong Consistency)
   - Write-Ahead Log with majority ack
   - Latency: ~200µs (~7x faster than traditional 3PC)
   - Use case: Critical data requiring durability
   - Configuration:
     config :super_cache, :wal,
       majority_timeout: 2_000,
       cleanup_interval: 5_000
""")

# ── 4. Write Operations (Distributed) ─────────────────────────────────────────

IO.puts("=== Distributed Write Operations ===\n")

# Writes are automatically routed to the primary node for each partition
IO.puts("Writing data (automatically routed to partition primary):")

# Tuple storage
SuperCache.put!({:user, 1, "Alice"})
SuperCache.put!({:user, 2, "Bob"})
SuperCache.put!({:user, 3, "Charlie"})
IO.puts("  Inserted 3 user tuples")

# Key-Value API
KeyValue.add("session", :user_1, %{name: "Alice", role: :admin})
KeyValue.add("session", :user_2, %{name: "Bob", role: :user})
IO.puts("  Added 2 sessions")

# Batch operations (efficient across partitions)
batch_data = for i <- 4..10, do: {:user, i, "User_#{i}"}
SuperCache.put_batch!(batch_data)
IO.puts("  Batch inserted 7 more users")

# Queue operations
Queue.add("jobs", "process_order_1")
Queue.add("jobs", "process_order_2")
IO.puts("  Added 2 jobs to queue")

# Stack operations
Stack.push("history", "page_a")
Stack.push("history", "page_b")
IO.puts("  Pushed 2 pages to stack")

IO.puts("")

# ── 5. Read Modes (Distributed) ───────────────────────────────────────────────

IO.puts("=== Read Modes ===\n")

IO.puts("""
SuperCache supports three read modes in distributed mode:

1. :local (Fastest, may be stale)
   - Reads from local node's ETS
   - Latency: ~10µs
   - Use when: Eventual consistency is acceptable

2. :primary (Consistent with primary)
   - Routes read to partition's primary node
   - Latency: ~100-300µs
   - Use when: Need to read your own writes

3. :quorum (Majority agreement with early termination)
   - Reads from multiple replicas, returns when majority agrees
   - Latency: ~100-200µs
   - Use when: Need strong read consistency
""")

# Demonstrate read modes
IO.puts("Reading with different modes:")

# Local read (default)
local_result = SuperCache.get!({:user, 1})
IO.puts("  Local read: #{inspect(local_result)}")

# Primary read
primary_result = SuperCache.get!({:user, 1}, read_mode: :primary)
IO.puts("  Primary read: #{inspect(primary_result)}")

# Quorum read
quorum_result = SuperCache.get!({:user, 1}, read_mode: :quorum)
IO.puts("  Quorum read: #{inspect(quorum_result)}")

# KeyValue with read modes
session_local = KeyValue.get("session", :user_1)
session_primary = KeyValue.get("session", :user_1, nil, read_mode: :primary)
IO.puts("  KV local: #{inspect(session_local)}")
IO.puts("  KV primary: #{inspect(session_primary)}")

IO.puts("")

# ── 6. Cluster Statistics & Health ────────────────────────────────────────────

IO.puts("=== Cluster Statistics & Health ===\n")

# Local stats
stats = SuperCache.stats()
IO.puts("Local cache stats:")
IO.puts("  #{inspect(stats)}")

# Cluster-wide stats (aggregates from all nodes)
cluster_stats = SuperCache.cluster_stats()
IO.puts("\nCluster stats:")
IO.puts("  #{inspect(cluster_stats)}")

# Health monitoring
IO.puts("\nHealth monitor available:")
IO.puts("  HealthMonitor.health() - Current cluster health")
IO.puts("  HealthMonitor.metrics() - Detailed metrics")
IO.puts("  Telemetry events: [:super_cache, :health, :check]")
IO.puts("  Telemetry events: [:super_cache, :health, :alert]")

IO.puts("")

# ── 7. Node Lifecycle Management ──────────────────────────────────────────────

IO.puts("=== Node Lifecycle Management ===\n")

IO.puts("""
Managing cluster nodes:

  # Manual node addition/removal
  SuperCache.Cluster.Manager.node_up(:"new_node@10.0.0.4")
  SuperCache.Cluster.Manager.node_down(:"old_node@10.0.0.1")

  # Force full partition sync (after node rejoin)
  SuperCache.Cluster.Manager.full_sync()

  # Automatic handling
  # - :nodeup/:nodedown events trigger automatic rebalancing
  # - NodeMonitor tracks node health and triggers sync
""")

IO.puts("Current live nodes: #{inspect(Manager.live_nodes())}")
IO.puts("")

# ── 8. Fault Tolerance Demonstration ──────────────────────────────────────────

IO.puts("=== Fault Tolerance ===\n")

IO.puts("""
SuperCache handles node failures gracefully:

1. Read Failover
   - If a node is down, reads route to available replicas
   - Quorum reads tolerate minority failures

2. Write Handling
   - Writes route to partition primary
   - If primary fails, partition is reassigned to replica

3. Node Rejoin
   - Automatic full sync when node rejoins cluster
   - Partition rebalancing based on current node list

4. Configuration Consistency
   - All nodes must have identical partition config
   - Late-joining nodes receive config from existing cluster
   - Mismatched configs are rejected
""")

# ── 9. Cleanup ────────────────────────────────────────────────────────────────

IO.puts("=== Cleanup ===\n")

# Clear all data across all nodes
SuperCache.delete_all()
IO.puts("Cleared all cache data")

# Stop SuperCache
SuperCache.stop()
IO.puts("SuperCache stopped")

IO.puts("\n=== Distributed Mode Example Complete ===")

IO.puts("""

Next Steps:
1. Start multiple BEAM nodes with --name and --cookie flags
2. Configure cluster_peers in config/runtime.exs
3. Set replication_mode based on your consistency needs
4. Use read_mode: :primary or :quorum for consistent reads
5. Monitor cluster health with HealthMonitor
""")
