import Config

config :logger,
  level: :info,
  backends: [:console],
  format: "[$level] $message $metadata\n",
  metadata: [:error_code, :file]
