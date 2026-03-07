# Guide for develop SuperCache

Clone [repo](https://github.com/ohhi-vn/super_cache)

Run:

```bash
mix deps.get
```

For using Tidewave:

```bash
mix tidewave
```

Go to [http://localhost:4000/tidewave](http://localhost:4000/tidewave)


## Using as local dependency

in `mix.exs` file add deps like:

```elixir
defp deps do
  base_dir = "/your/base/path"

  [
    {:super_cache, path: Path.join(base_dir, "super_cache")}
    
    # or using git repo, using override: true if you want to override in sub deps.
    # {:super_cache, git: "https://github.com/your_account/super_cache", override: true}
  ]
end
```

## Usage guide in plain iex (single cluster, two nodes)

Scripts for test & run in iex.

Start cluster:

```bash
iex --name node1@127.0.0.1 --cookie need_to_change_this -S mix
```

```bash
iex --name node2@127.0.0.1 --cookie need_to_change_this -S mix
```

Join cluster (or using lib cluster for your code):

```Elixir
# -- node1@127.0.0.1 ---
 
Node.connect(:"node2@127.0.0.1")

# need start cache if :auto_start is missed or equal false.
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed, replication_factor: 2,
  num_partition: 3
)

# Verify partition map is built
SuperCache.Cluster.Manager.live_nodes()

alias SuperCache.Distributed, as: DCache
DCache.put!({:hello, "world"})

# Check record counts per partition on each node
SuperCache.stats()
```

```elixir
# --- node2@127.0.0.1 ---

# verify node 1 is joined
Node.list()

SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed, replication_factor: 2
)

alias SuperCache.Distributed, as: DCache
DCache.get!({:hello, nil})
#=> [{:hello, "world"}]           ← replicated from node1

```

## Test

Depends on your test type. run below scripts.

```bash
# Unit tests — no distribution needed
mix test

# Cluster tests only
mix test.cluster
```

## Benchmark & Profiling guide

Scripts for benchmark & profiling in `./tools' folder.
