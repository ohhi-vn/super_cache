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
write 8000000 records need 11.74s, 681647.69
read 8000000 records need 1.92s, 4157133.41
mix read/write 8000000 records need 2.89s, 2770390.07
```

Read/write direct on ETS:

```
write 8000000 records need 2.05s, 3905079.24
read 8000000 records need 1.33s, 5993941.62
mix read/write  8000000 records need 2.06s, 3884690.73
```