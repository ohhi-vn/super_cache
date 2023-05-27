# Benchmark

## Hardware

Run on Windows 11 Pro x64

CPU AMD 3700x (8C/16T)

RAM 64GB 2600

worker process: 16

## Script test

Script is available at tools folder

## Result

SuperCache:

```
write          16000000 records need 24.28s, 658985.21 req/s
read           16000000 records need 4.23s, 3785034.64 req/s
mix read/write 16000000 records need 4.37s, 3661480.56 req/s
```

Read/write direct on ETS:

```
write              16000000 records need 1.33s, 12060980.32 req/s
read               16000000 records need 1.21s, 13196790.54 req/s
mix read/write ets 16000000 records need 1.1s, 14547067.95 req/s
```