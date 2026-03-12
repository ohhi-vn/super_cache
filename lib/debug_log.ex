# =============================================================================
# lib/super_cache/log.ex
#
# Conditional debug logging.  All internal Logger.debug calls now go through
# this macro so they are zero-cost when debug logging is disabled.
#
# Enable at application start:
#   config :super_cache, debug_log: true
# =============================================================================
defmodule SuperCache.Log do
  @moduledoc """
  Conditional debug logging for SuperCache.

  Debug output is suppressed by default.  Enable in two ways:

  **Application config** (evaluated at runtime):

      # config/config.exs
      config :super_cache, debug_log: true

  All internal `Logger.debug/1` calls are routed through `SuperCache.Log.debug/1`,
  which is a no-op when disabled so no message strings are built in production.
  """

  require Logger

 @enable_debug_log  Application.compile_env(:super_cache, :debug_log, false)

  @doc """
  Emit a debug log if debug logging is enabled.

  Accepts the same forms as `Logger.debug/1`: a string, iodata, or a
  zero-arity anonymous function (lazy evaluation).
  """
  defmacro debug(chardata_or_fun) do
    if @enable_debug_log do
      quote do
        Logger.debug(unquote(chardata_or_fun))
      end
    end
  end
end
