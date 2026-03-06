# Distributed Cache

Support for distributing data cross cluster.

Note: Still in development phase & unstable.

## Config

For running distruted mode need add same config for all nodes.

```elixir
# config/config.exs
config :super_cache,
  auto_start:         true,
  key_pos:            0,
  partition_pos:      0,
  cluster:            :distributed,
  replication_factor: 2,       # primary + 1 replica
  table_type:         :set,
  num_partition:      8        # fix this so all nodes agree; do NOT leave it
```

Need add config for `SuperCache` can join nodes.

```elixir
# config/runtime.exs
import Config

config :super_cache,
  cluster_peers: [
    :"node1@10.0.0.1",
    :"node2@10.0.0.2",
    :"node3@10.0.0.3"
  ]
```

Application will auto start and join cluster.
