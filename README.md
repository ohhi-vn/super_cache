[![Docs](https://img.shields.io/badge/api-docs-green.svg?style=flat)](https://hexdocs.pm/super_cache)
[![Hex.pm](https://img.shields.io/hexpm/v/super_cache.svg?style=flat&color=blue)](https://hex.pm/packages/super_cache)

# SuperCache

High-performance in-memory caching library for Elixir backed by partitioned ETS
tables with experimental distributed cluster support. Transparent local and
distributed modes, configurable consistency (async / sync / strong / Raft),
batch operations, and multiple data structures.

## Features

- **Partitioned ETS storage** — splits data across N tables to reduce contention
- **Multiple data structures** — tuples, key-value namespaces, queues, stacks, structs
- **Distributed clustering** — node discovery, partition assignment, replication
- **Configurable consistency** — async, sync (quorum), strong (WAL), opt-in Raft
- **Batch operations** — `put_batch!/1`, `add_batch/2`, `remove_batch/2`
- **Performance optimized** — compile-time log elimination, inlined routing, worker pools
- **Health monitoring** — cluster health, replication lag, telemetry events

## Installation

Requires Erlang/OTP 25+ and Elixir 1.15+.

```elixir
def deps do
  [{:super_cache, "~> 1.2"}]
end
```

## Quick Start

```elixir
SuperCache.start!()

SuperCache.put!({:user, 1, "Alice"})
SuperCache.get!({:user, 1})
# => [{:user, 1, "Alice"}]

# Key-value namespace
alias SuperCache.KeyValue
KeyValue.add("session", :user_1, %{name: "Alice"})
KeyValue.get("session", :user_1)
```

See [Usage Guide](guides/Usage.md) for the complete API reference and
[Distributed Guide](guides/Distributed.md) for cluster setup.

## Performance

| Operation | Throughput |
|-----------|-----------|
| `put!` (local) | ~1.2M ops/sec |
| `get!` (local) | ~2.1M ops/sec |
| `KeyValue.add_batch` (10k) | ~1.1M ops/sec |

### Distributed modes (3 nodes, µs per operation)

Lower is better. Raft figures use a real replicated group (2 or 3 voters).

| Mode | RF | `put!` | `get! :primary` | `get! :quorum` |
|------|----|--------|-----------------|----------------|
| `:async` | 2 | 23 µs | 0.76 µs | 78 µs |
| `:sync` | 2 | 106 µs | 0.45 µs | 60 µs |
| `:strong` | 2 | 106 µs | 0.46 µs | 57 µs |
| `:raft` | 2 | 1,111 µs | 5.63 µs | 72 µs |
| `:async` | 3 | 16 µs | 1.08 µs | 101 µs |
| `:sync` | 3 | 102 µs | 0.47 µs | 79 µs |
| `:strong` | 3 | 122 µs | 0.42 µs | 82 µs |
| `:raft` | 3 | 6,768 µs | 48.49 µs | 82 µs |

Raft writes are fsync- and quorum-bound (~8–10× slower than `strong` with two
voters, ~50× with three). Raft primary reads also pay a query barrier and, when
the client is not the leader, an RPC round-trip. Quorum reads cost about the
same in every mode. Write latency is roughly flat as cluster size grows once
the voter count is fixed.

Distributed latency and cluster-size scaling (1 / 3 / 5 nodes) are recorded in
`benchmarks/results/distributed-scaling.md`; the full Raft comparison and the
multi-voter membership fix are documented in
`benchmarks/results/raft-comparison.md`.

## Guides

- [Usage Guide](guides/Usage.md) — complete API reference and examples
- [Distributed Guide](guides/Distributed.md) — clustering, replication, read modes, health
- [Developer Guide](guides/Developer.md) — architecture, testing, benchmarking, contributing

## Examples

```bash
mix run examples/local_mode_example.exs
mix run examples/distributed_mode_example.exs
```

## AI Agent Skills

Machine-readable skill files live under [`skills/`](skills/). See that
directory for installation into consuming apps.

## License

MIT License. See [LICENSE](LICENSE) for details.