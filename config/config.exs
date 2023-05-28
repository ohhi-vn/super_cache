import Config

config :logger,
  level: :info,
  backends: [:console],
  format: "[$level] $message $metadata\n",
  metadata: [:error_code, :file],
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]
