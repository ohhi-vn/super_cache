import Config

config :logger, level: :debug

config :super_cache, debug_log: false

# for start with application
# config :super_cache,
#   auto_start:         true,
#   key_pos:            0,
#   partition_pos:      0,
#   cluster:            :distributed,
#   replication_mode:   :strong,
#   replication_factor: 2,       # primary + 1 replica
#   table_type:         :set,
#   num_partition:      8        # fix this so all nodes agree; do NOT leave it
#                                # as "auto" (scheduler count) in cluster mode
#                                # or different hardware gives different values

# config :super_cache,
#   cluster_peers: [
#     :"node1@127.0.0.1",
#     :"node2@127.0.0.1",
#     :"node3@127.0.0.1"
#   ]
