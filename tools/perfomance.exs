
alias :ets, as: Ets

SuperCache.start()
num = 500_000
worker = 30
table_name = :test_direct

Ets.new(table_name, [
  :bag,
  :public,
  :named_table,
  {:keypos, 1},
  {:write_concurrency, true},
  {:read_concurrency, true},
  {:decentralized_counters, true}
])

fun_direct_write = fn start, stop ->
  for i <- start..stop, do: Ets.insert(table_name, {i, :a})
end

fun_direct_read = fn start, stop ->
  for i <- start..stop, do: Ets.lookup(table_name, i)
end

fun_direct_mix = fn start, stop ->
  for i <- start..stop do
    if div(i, 3) == 0 do
      Ets.insert(table_name, {i, :a})
    else
      Ets.lookup(table_name, i)
    end
  end
end

fun_write = fn start, stop ->
  for i <- start..stop, do: SuperCache.put({i, :a})
end

fun_lazy_write = fn start, stop ->
  for i <- start..stop, do: SuperCache.lazy_put({i, :a})
end

fun_read = fn start, stop ->
  for i <- start..stop, do: SuperCache.get_same_key_partition!(i)
end

fun_mix = fn start, stop ->
  for i <- start..stop do
    if div(i, 3) == 0 do
      SuperCache.put({i, :a})
    else
      SuperCache.get_same_key_partition!(i)
    end
  end
end

# warm up CPU
{_, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_direct_write.(i * num, div((i + 1) * num, 2))
      end)
    end
  Task.await_many(list, 120_000)
end)

Ets.delete_all_objects(table_name)

{direct_write_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_direct_write.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "Direct write ets #{num * worker} records need #{inspect Float.round(direct_write_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/direct_write_time, 2)} req/s"

{direct_read_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_direct_read.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "Direct read ets #{num * worker} records need #{inspect Float.round(direct_read_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/direct_read_time, 2)} req/s"

{direct_mix_time, _} = :timer.tc(fn ->
  Ets.delete_all_objects(table_name)
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_direct_mix.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "Direct mix read/write ets #{num * worker} records need #{inspect Float.round(direct_mix_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/direct_mix_time, 2)} req/s"
IO.puts ""

Ets.delete_all_objects(table_name)

{write_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_lazy_write.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "SuperCache lazy write #{num * worker} records need #{inspect Float.round(write_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/write_time, 2)} req/s"


SuperCache.delete_all()

{write_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_write.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "SuperCache write #{num * worker} records need #{inspect Float.round(write_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/write_time, 2)} req/s"

{read_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_read.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "SuperCache read #{num * worker} records need #{inspect Float.round(read_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/read_time, 2)} req/s"

{mix_time, _} = :timer.tc(fn ->
  SuperCache.delete_all()
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_mix.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 120_000)
end)

IO.puts "SuperCache mix read/write #{num * worker} records need #{inspect Float.round(mix_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/mix_time, 2)} req/s"
IO.puts ""

SuperCache.delete_all()

# break for test req/s only
raise "stop"

##  test with Benchee

list = 1..num

read_1p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 1)

read_5p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 5)

read_10p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 10)

write_1p = Task.async_stream(list, fn index ->
  {:ok, SuperCache.put({index, :a})}
end, order: false, max_concurrent: 1)

write_5p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.put({index, :a})}
end, order: false, max_concurrent: 5)

write_10p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.put({index, :a})}
end, order: false, max_concurrent: 10)

Benchee.run(%{
  "write 1 process" => fn -> Enum.to_list(write_1p) end,
  "write 5 processes" => fn -> Enum.to_list(write_5p) end,
  "write 10 processes" => fn -> Enum.to_list(write_10p) end
  })


Benchee.run(%{
  "read 1 process" => fn -> Enum.to_list(read_1p) end,
  "read 5 processes" => fn -> Enum.to_list(read_5p) end,
  "read 10 processes" => fn -> Enum.to_list(read_10p) end
  })
