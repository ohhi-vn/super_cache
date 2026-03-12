[![Docs](https://img.shields.io/badge/api-docs-green.svg?style=flat)](https://hexdocs.pm/super_cache)
[![Hex.pm](https://img.shields.io/hexpm/v/super_cache.svg?style=flat&color=blue)](https://hex.pm/packages/super_cache)

# SuperCache

## Introduce

This is cache library for Elixir based on Ets table.
Library is to run as standalone or distributed (experiment).

Support to store tuple, struct, key/value, queue & stack for sharing state between processes.

## Design

Client -> API -> Partition -> Storage

Current version, library has three main part:
1. API interface & config holder.
2. Partition holder.
3. Storage partition.

### API interface:

Client interacts with library throw qua api interface.
All config after start will be hold in this part.

### Partition

Support api get right storage part for data.

Partition based on Erlang phash2/2.
Piece data after extract with config's info will get order of partition.
Order is used get target partition.

All partitions will be calculated after client call start/n start!/n function.

### Storage

Storage data of client. Number of storage partition is same with number of partition above.

Core of storage is Ets table.

### call flow of api

Sequencer flow of api (on a node):

```mermaid
sequenceDiagram
  participant Client
  participant Api
  participant Partition
  participant Storage

  Client->>Api: Add new tuple to cache
  Api->>Partition: Get partition
  Partition->>Api: Your partition
  Api->>Storage: Put new/update tuple
  Storage->>Api: Result
  Api->>Client: Result
  
  Client->>Api: Get data for key/pattern
  Api->>Partition: Get partition
  Partition->>Api: Your patition
  Api->>Storage: Get data for key/pattern
  Storage->>Api: Data for key
  Api->>Client: Your data
```

(If diagram doesn't show, please install mermaid support extension for IDE)

Simple module flow api:

```mermaid
graph LR
Client(Client) --> Api(Api)
    Api-->|get partition|Part(Partition holder)
    Api-->|Partition1| E1(Partition Storage 1)
    Api-->|Partition2| E2(Partition Storage 2)
```

## Installation

Requiremnt: Erlang/OTP version 25 or later.

Library can be installed
by adding `super_cache` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:super_cache, "~> 1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/super_cache>.

## Guide

Start SuperCache with default config:

```elixir
SuperCache.start!()
```

*(key_pos = partition_pos = 0, table_type = :set, num_partition = on_line schedulers of Erlang VM)*

Start with config:

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 3]
SuperCache.start!(opts)
```

Note:

1. key_pos: Key's position of tuple use to lookup in Ets table.

2. partition_pos: Position of element in tuple is used to calculate partition for store & lookup.

3. table_type: Type of Ets table.

4. num_partition: Number of partitions (= number of Ets table).

Basic usage:

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 3]
SuperCache.start!(opts)

SuperCache.put!({:hello, :world, "hello world!"})

SuperCache.get_by_key_partition!(:hello, :world)

SuperCache.delete_by_key_partition!(:hello, :world)
```

KeyValue usage:

```elixir
  alias SuperCache.KeyValue

  # Start cache
  SuperCache.start!()

  KeyValue.add("my_kv", :key, "Hello")
  KeyValue.get("my_kv", :key)
    # => "Hello"

  KeyValue.remove("my_kv", :key)
  KeyValue.get("my_kv", :key)
    # => nil

  KeyValue.add("my_kv", :key, "Hello")
  KeyValue.remove_all("my_kv")
```

Queue usage:

```elixir
alias SuperCache.Queue

# Start cache
SuperCache.start!()

Queue.add("my_queue", "Hello")
Queue.out("my_queue")
  # => "Hello"
```

Stack usage:

```elixir
alias SuperCache.Stack

# Start cache
SuperCache.start!()

Stack.push("my_stack", "Hello")
Stack.pop("my_stack")
  # => "Hello"
```

Struct storage usage:

```elixir
alias SuperCache.Struct

# Start cache
SuperCache.start!()

# Init key storage for struct.
Struct.init(%MyStruct{}, :id)

a = %MyStruct{id: 1, data: :a}

Struct.add(a)

# get struct
{:ok, result} = Struct.get(%MyStruct{id: 1})
  # =>  %MyStruct{id: 1, data: :a}
```

## Distributed Cache

Can use config or manual start.
Remember number of partition in each node must be same.

config:

```elixir

# for start with application
config :super_cache,
  auto_start:         true,
  key_pos:            0,
  partition_pos:      0,
  cluster:            :distributed,
  replication_factor: 2,       # primary + 1 replica
  table_type:         :set,
  num_partition:      3        # fix this so all nodes agree

config :super_cache,
  cluster_peers: [
    :"node1@127.0.0.1",
    :"node2@127.0.0.1",
    :"node3@127.0.0.1"
  ]
```

Manual start cache in distributed mode. Go to `SuperCache.Cluster.Bootstrap` for more details.

```elixir
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0,
  partition_pos: 0,
  cluster: :distributed,
  replication_factor: 2,
  num_partition: 3   
)
```

in a node can add data like:

```elixir
SuperCache.Distributed.put!({:hello, "world"})
```

access from other node:

```elixir
SuperCache.Distributed.get!({:hello, nil})
#=> [{:hello, "world"}]           ← replicated from node1
```

Distributed mode has same api like standalone.
Check docs in `SuperCache.Distributed.{Stack, Queue, KeyValue, Struct}` module for more details.

Other APIs please go to document on [hexdocs.pm](https://hexdocs.pm/super_cache)
