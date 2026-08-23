---
name: super-cache-collections
description: Use when an application depending on super_cache needs namespaced higher-level data structures — SuperCache.KeyValue (key/value with counters and batch ops), SuperCache.Queue (FIFO), SuperCache.Stack (LIFO), or SuperCache.Struct (store Elixir structs keyed by a field). Load this before generating calls to KeyValue/Queue/Stack/Struct modules.
---

# SuperCache Collections — KeyValue, Queue, Stack, Struct

These four modules wrap the raw tuple cache with namespaced, typed
operations. All names are **atoms** (or binaries where noted). They work in
local mode and, transparently, in distributed mode.

## KeyValue — `SuperCache.KeyValue`

Hash-map semantics scoped per `kv_name` namespace.

```elixir
alias SuperCache.KeyValue

KeyValue.add("sessions", "tok123", %{user_id: 7})   # true — upsert (insert or overwrite)
KeyValue.get("sessions", "tok123")                  # value | default(nil)
KeyValue.get("sessions", "miss", :fallback)         # :fallback
KeyValue.get_all("sessions", "tok123")              # all values for key (bag tables)

KeyValue.update("counters", "logins", 0)            # upsert, returns :ok
KeyValue.update("counters", "logins", 0, fn v -> v + 1 end)  # read-modify-write,
                                                    # returns the NEW value
KeyValue.increment("counters", "hits")              # default 0, step 1 → new count
KeyValue.increment("counters", "credits", 100, -5)  # custom default & step
KeyValue.replace("sessions", "tok123", %{user_id: 8})  # :ok — upsert (inserts if missing)

KeyValue.keys("sessions")        # list of keys
KeyValue.values("sessions")      # list of values
KeyValue.count("sessions")       # number of entries
KV = KeyValue.to_list("sessions")               # [{key, value}]
KeyValue.remove("sessions", "tok123")
KeyValue.remove_all("sessions")

# Batch (one routed operation per partition — prefer over loops):
KeyValue.add_batch("sessions", [{"t1", %{}}, {"t2", %{}}])
KeyValue.remove_batch("sessions", ["t1", "t2"])
```

Notes:
- `add/3` and `replace/3` are upserts and always return `true`; `get/4`
  returns the most recent value for a key (bag tables may hold several —
  use `get_all/3`).
- `update/4` fun variant returns the new **value**, not `:ok`.
- Names may be atoms or binaries; they become part of the internal record key.

## Queue — `SuperCache.Queue`

FIFO per `queue_name`.

```elixir
alias SuperCache.Queue

Queue.add(:jobs, %{"task" => "email"})
Queue.peak(:jobs)                # peek head without removing (default nil)
Queue.out(:jobs)                 # pop head (default nil when empty)
Queue.count(:jobs)
Queue.get_all(:jobs)             # ⚠ DRAINS — returns remaining items AND empties the queue
```

## Stack — `SuperCache.Stack`

LIFO per `stack_name`.

```elixir
alias SuperCache.Stack

Stack.push(:undo, {:edit, path_before})
Stack.pop(:undo)                 # most recent item, or default
Stack.pop(:undo, :empty)         # custom default
Stack.count(:undo)
Stack.get_all(:undo)             # ⚠ DRAINS — resets the stack and returns its items
```

## Gotchas

- `Queue.get_all/1` and `Stack.get_all/1` **empty** the structure — they are
  drain operations, not snapshots. Use `peak/2` + `out/2` (or `count/2`)
  when you must keep contents.
- `Stack.pop/1` on a stack whose counter record was never initialized returns
  the default; it self-heals on next `push/2`.

## Struct — `SuperCache.Struct`

Persist Elixir structs, keyed by one of their fields. One struct type per
namespace; call `init/2` once before adding instances.

```elixir
defmodule Player do
  defstruct [:id, :name]
end

player = %Player{id: 7, name: "Ana"}

Struct.init(player, :id)         # register :id as the key field — once per type
Struct.add(%Player{id: 7, name: "Ana"})     # upsert instance
Struct.get(%Player{id: 7})       # => {:ok, %Player{...}} | {:error, _}
Struct.get_all(%Player{})        # every stored instance of this type
Struct.remove(%Player{id: 7})
Struct.remove_all(%Player{})     # remove ALL instances of this type
```

## Distributed behaviour

In distributed mode these modules route writes to the owning primary node and
read locally by default. Strong-consistency variants exist as explicit
functions (`Queue.dist_enqueue/dist_dequeue/dist_peek/dist_count/dist_drain`,
`Stack.dist_push/dist_pop`) — see the `super-cache-distributed` skill.
