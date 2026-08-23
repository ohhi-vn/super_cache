# Extract uncovered lines per target module from cover/*.html.
# Usage: mix test --cover && elixir -S mix run tools/coverage/uncovered.exs

targets = %{
  "SuperCache.Buffer" => "lib/buffer/buffer.ex",
  "SuperCache.Stack" => "lib/api/stack.ex",
  "SuperCache.Queue" => "lib/api/queue.ex",
  "SuperCache.KeyValue" => "lib/api/key_value.ex",
  "SuperCache.Struct" => "lib/api/struct.ex",
  "SuperCache" => "lib/api/super_cache.ex",
  "SuperCache.Internal.Stream" => "lib/buffer/stream.ex",
  "SuperCache.Internal.Queue" => "lib/buffer/queue.ex",
  "SuperCache.Bootstrap" => "lib/bootstrap.ex",
  "SuperCache.Cluster.Bootstrap" => "lib/cluster/cluster_bootstrap.ex",
  "SuperCache.Cluster.HealthMonitor" => "lib/cluster/health_monitor.ex",
  "SuperCache.Cluster.Replicator" => "lib/cluster/replicator.ex",
  "SuperCache.Cluster.WAL" => "lib/cluster/wal.ex",
  "SuperCache.Cluster.Router" => "lib/cluster/router.ex",
  "SuperCache.Cluster.ThreePhaseCommit" => "lib/cluster/three_phase_commit.ex",
  "SuperCache.Config" => "lib/app/config.ex",
  "SuperCache.Sup" => "lib/app/sup.ex"
}

for {mod_name, src} <- Enum.sort(targets) do
  file = "cover/Elixir.#{mod_name}.html"

  if File.exists?(file) do
    html = File.read!(file)

    misses =
      Regex.scan(~r{<tr class="miss">\s*<td class="line" id="L(\d+)"}, html)
      |> Enum.map(&String.to_integer(Enum.at(&1, 1)))
      |> Enum.uniq()

    lines = File.read!(src) |> String.split("\n")

    if misses != [] do
      IO.puts("\n=== #{mod_name} (#{src}) — #{length(misses)} uncovered: #{Enum.join(misses, ",")}")

      Enum.each(misses, fn ln ->
        src_line = lines |> Enum.at(ln - 1) |> to_string() |> String.trim()

        IO.puts("   #{String.pad_leading(Integer.to_string(ln), 4)}: #{src_line}")
      end)
    end
  else
    IO.puts("\n=== #{mod_name}: no html report at #{file}")
  end
end

System.halt(0)
