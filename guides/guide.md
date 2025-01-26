# Guide

SuperCache is similar to Ets for easy to use.
Current version, it's support limited function from Ets, ex: insert, lookup, delete, match_object, scan object.

Start SuperCache with default config:

```elixir
SuperCache.start!()
```

*default config: [key_pos = partition_pos = 0, table_type = :set, num_partition = online schedulers of Erlang VM]*

Start with config:

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 3]
SuperCache.start!(opts)
```

Note:

1. key_pos: Key's position of tuple use to lookup in Ets table.

2. partition_pos: Position of element in tuple is used to calculate partition for store & lookup.

3. table_type: Type of Ets table.

4. num_partition: Number of partitions (= number of Ets table) on a node.

In almost case `key_pos` and `partition_pos` is same.
If you need to organize data for fast access you can choose right data to calculate partition.
Each partition have one or more replicated partition for local access & backup.

Basic usage:

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 3]
SuperCache.start!(opts)

SuperCache.put!({:hello, :world, "hello world!"})

SuperCache.get_by_key_partition!(:hello, :world)

SuperCache.delete_by_key_partition!(:hello, :world)

SuperCache.put({:a, :b, 1, 3, "c"})

SuperCache.get_by_match_object!({:_, :_, 1, :_, "c"})
[{:a, :b, 1, 3, "c"}]

```

In case, key_pos & partition_pos is same:

```elixir
SuperCache.put({:hello, :world, :example})
SuperCache.get_same_key_partition!(:hello)
SuperCache.delete
```
