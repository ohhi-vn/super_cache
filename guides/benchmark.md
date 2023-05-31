# Benchmark

## Hardware

Run on Windows 11 Pro x64

CPU AMD 3700x (8C/16T)

RAM 64GB 2600

## Script test

Ets table config for both:

```elixir
  ...
  {:write_concurrency, true},
  {:read_concurrency, true},
  {:decentralized_counters, true}
  ...
```

Script is available at tools folder

## Result

### 16 worker processes

16 processes are send requests to target.

SuperCache:

```
lazy write     32_000_000 records need 8.93s, 3_582_236.85 req/s
write          32_000_000 records need 83.5s, 383_226.81 req/s
read           32_000_000 records need 8.66s, 3_693_853.43 req/s
mix read/write 32_000_000 records need 10.88s, 2_942_312.02 req/s
```

Read/write direct on one Ets table:

```
write          32_000_000 records need 8.71s, 3_673_013.63 req/s
read           32_000_000 records need 6.05s, 5_291_856.64 req/s
mix read/write 32_000_000 records need 9.76s, 3_277_055.37 req/s
```

### 40 worker processes