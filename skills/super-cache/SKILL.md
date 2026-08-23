---
name: super-cache
description: Use when writing or reviewing Elixir code in an application that depends on the super_cache hex package — covers starting/stopping the cache, the tuple record model (and its key-collision trap), put/get/delete/scan operations, batch writes, lazy_put, error handling conventions, and stats. Load this before generating any SuperCache.* API call.
---

# SuperCache — Core Usage

SuperCache is an in-memory ETS cache. Records are **plain tuples**; reads are
near raw-ETS speed (~2M ops/sec). Everything lives under the `SuperCache`
module for local mode; `SuperCache.Distributed` is a deprecated alias of it.

## Lifecycle

```elixir
# In your app's supervision tree via config (recommended):
#   config :super_cache, auto_start: true, key_pos: 0, partition_pos: 0, num_partition: 8

SuperCache.start!(key_pos: 0, partition_pos: 0, num_partition: 8)  # raises on bad opts
SuperCache.start(key_pos: 0, partition_pos: 0)                     # {:ok, _} | {:error, _}
SuperCache.started?()      # => true | false (never exits — safe before start)
SuperCache.stop()          # frees all ETS memory
```

Required options: `:key_pos`, `:partition_pos` (non-negative integers).
`SuperCache.start!/0` uses library defaults. Other options: `:num_partition`
(default = scheduler count), `:table_type` (`:set`, `:ordered_set`, `:bag`,
`:duplicate_bag`; default `:set`).

## The record model — read this before writing any put!

A record is a tuple. The element at index `key_pos` (default **0**) is the
**ETS key**: it is the record's identity. The element at `partition_pos`
controls which partition table holds the record.

**CRITICAL gotcha:** with defaults, these five puts leave exactly ONE record,
because every tuple's first element is `:user`:

```elixir
for i <- 1..5, do: SuperCache.put!({:user, i, "data_#{i}"})
# scan count == 1 — records overwrote each other!
```

Correct patterns:

```elixir
# Pattern A — composite key as element 0 (no extra config needed):
SuperCache.put!({{:user, 42}, %{name: "Alice", score: 10}})
SuperCache.get!({{:user, 42}, nil})          # => [{{:user, 42}, %{...}}]

# Pattern B — unique atom/binary key:
SuperCache.put!({"session:abc123", %{user_id: 7, expires: ~U[...]}})
SuperCache.get!("session:abc123")            # get tuple's elem(0) is the lookup key

# Pattern C — configure key_pos to point at the identity field:
SuperCache.start!(key_pos: 1, partition_pos: 1, num_partition: 8)
SuperCache.put!({:user, 42, "Alice"})        # key = 42, partitioned by 42
SuperCache.get!({:user, 42, nil})            # key = 42
```

Rule of thumb: **the value at `key_pos` must be unique per logical record.**
If it is not, use a composite tuple as element 0 or reconfigure `:key_pos`.

## Reads return lists

All `get*` functions return a **list of stored tuples** (empty list on miss),
never `nil` and never a bare record:

```elixir
case SuperCache.get!(my_key) do
  [{^my_key, value}] -> value          # :set tables hold at most one per key
  [] -> :not_found
end
```

## Core operations

```elixir
# Write (bang versions raise on failure; non-bang return {:ok,_} | {:error,_})
SuperCache.put!(record_tuple)
SuperCache.put(record_tuple)
SuperCache.put_batch!([t1, t2, t3])    # groups by partition — much faster than N puts
SuperCache.lazy_put(tuple)             # buffered async write (needs buffers running;
                                       # dropped silently if buffers are stopped)

# Read
SuperCache.get!(lookup_tuple)                          # local partition read
SuperCache.get_by_match!(:_, {:"user:*-prefix-pattern", :_, :_}, ...)
SuperCache.scan!(:_, fn record, acc -> [record | acc] end, [])   # full-table fold

# Delete
SuperCache.delete!(lookup_tuple)       # deletes by elem(key_pos)
SuperCache.delete_all()                # wipes every partition — destructive!
SuperCache.delete_by_match!(:_, pattern)

# Introspection
SuperCache.stats()                     # keyword list: per-partition counts + total:
                                       # [total: n, "SuperCache.Storage.Ets_0": n0, ...]
SuperCache.distributed?()
```

Match patterns use Erlang match-spec style atoms (`:_` wildcard, `:"$1"`
captures). Example — all records whose element 1 equals `:active`:

```elixir
pattern = {:"$1", :active}                       # {key, status} shaped records
SuperCache.get_by_match_object!(:_, pattern)     # => matching records
```

## Error-handling convention

Every `name!/arity` raises; the same function without `!` returns
`{:ok, result}` or `{:error, reason}`. Prefer bang versions inside `with`/
`try` blocks you control; prefer non-bang at boundaries.

## Common pitfalls

1. **Key collisions** — see the record-model section above.
2. **get returns a list** — pattern-match `[record]`, don't treat it as the record.
3. **`delete_all/0` is global** — never call it in request paths; test cleanup only.
4. **`lazy_put/1` needs buffers** — after `Bootstrap.stop()` buffered writes are dropped; use `put!/1` when durability matters.
5. **Tuples only** — `put!` accepts tuples; wrap maps/structs via the Struct API (see super-cache-collections skill) or store `%{...}` as a tuple field.
6. **No TTL support** — expiry must be application-managed (store timestamps, sweep with `delete_by_match!/2`).

## Where to look next

- Collections (KeyValue / Queue / Stack / Struct): load the `super-cache-collections` skill
- Clustering, replication & health: load the `super-cache-distributed` skill
- Full guides live in this repo: `guides/Usage.md`, `guides/Distributed.md`
