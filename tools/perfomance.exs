SuperCache.start()

for i <- 1..50_000, do: SuperCache.put({i, :a})

SuperCache.put({:foo, "lorem_ipsum"})


list = 1..50_000

read_1p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 1)

read_5p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 5)

read_10p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.get_same_key_partition!(index)}
end, order: false, max_concurrent: 100)

write_1p = Task.async_stream(list, fn index ->
  {:ok, SuperCache.put({index, :a})}
end, order: false, max_concurrent: 1)

write_5p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.put({index, :a})}
end, order: false, max_concurrent: 5)

write_10p = Task.async_stream(list, fn index ->
  {:ok,  SuperCache.put({index, :a})}
end, order: false, max_concurrent: 100)

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
