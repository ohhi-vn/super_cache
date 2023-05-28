  ## Examples

      iex> opts = [key_pos: 0, partition_pos: 0, table_type: :bag, num_partition: 3]
      iex> SuperCache.start(opts)
      iex> SuperCache.put({:hello, :world, "hello world!"})
      iex> SuperCache.get_by_key_partition!(:hello, :world)
      iex> SuperCache.delete_by_key_partition!(:hello, :world)