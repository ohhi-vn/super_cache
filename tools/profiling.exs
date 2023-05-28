SuperCache.start()
num = 100_000
worker = 16

fun_write = fn start, stop ->
  for i <- start..stop, do: SuperCache.lazy_put({i, :a})
end

:eprof.start_profiling([self()])

{write_time, _} = :timer.tc(fn ->
  list =
    for i <- 1..worker do
      Task.async( fn ->
        fun_write.(i * num, (i + 1) * num)
      end)
    end
  Task.await_many(list, 60_000)
end)

:eprof.stop_profiling()
:eprof.analyze()

IO.puts "SuperCache write #{num * worker} records need #{inspect Float.round(write_time/1_000_000, 2)}s, #{inspect Float.round(1_000_000 * num * worker/write_time, 2)} req/s"
