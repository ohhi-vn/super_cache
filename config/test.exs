import Config

# Exercise every logging path under test: level :debug makes Logger evaluate
# lazy message functions, while disabling the default handler keeps the
# output silent (ExUnit.CaptureLog still captures via its own handler).
config :logger, level: :debug, default_handler: false

config :super_cache, debug_log: false
