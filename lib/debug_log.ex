# =============================================================================
# lib/debug_log.ex
#
# Conditional logging for SuperCache.
#
# All internal log calls go through this module so that:
#   • Debug output is zero-cost when disabled (no string building).
#   • The toggle can be changed at runtime via `enable/1`.
#   • String, iodata, and zero-arity fun arguments are all supported.
#
# Enable at compile time (zero overhead in production):
#   config :super_cache, debug_log: true
#
# Or toggle at runtime:
#   SuperCache.Log.enable(true)
# =============================================================================
defmodule SuperCache.Log do
  @moduledoc """
  Conditional logging for SuperCache.

  Debug output is suppressed by default.  Enable in two ways:

  **Compile-time** (evaluated at compile time, zero overhead when disabled):

      # config/config.exs
      config :super_cache, debug_log: true

  **Runtime** (can be toggled while the system is running):

      SuperCache.Log.enable(true)

  All internal logging calls are routed through this module so that when
  debug is disabled, no message strings are built and no function calls
  are made — the macro expands to `:ok`.

  ## Examples

      require SuperCache.Log

      # Lazy evaluation — only called when debug is enabled
      SuperCache.Log.debug(fn -> "expensive: \#{inspect(large_data)}" end)

      # Direct string or iodata
      SuperCache.Log.debug("simple message")
      SuperCache.Log.info("operation completed")
  """

  require Logger

  @compile {:inline, enabled?: 0}

  # Compile-time flag — when false, all debug macros expand to :ok.
  @enable_debug_log Application.compile_env(:super_cache, :debug_log, false)

  # Runtime toggle stored in persistent_term for zero-overhead reads on the hot path.
  @pt_enabled {__MODULE__, :debug_enabled}

  @doc """
  Enable or disable debug logging at runtime.

  This overrides the compile-time setting.  Pass `true` to enable, `false`
  to disable.

  ## Examples

      SuperCache.Log.enable(true)
      SuperCache.Log.enable(false)
  """
  @spec enable(boolean) :: :ok
  def enable(enabled) when is_boolean(enabled) do
    :persistent_term.put(@pt_enabled, enabled)
    :ok
  end

  @doc """
  Returns whether debug logging is currently enabled.

  Checks the runtime toggle first; if not set, falls back to the
  compile-time configuration.
  """
  @spec enabled?() :: boolean
  @compile {:inline, enabled?: 0}
  def enabled?() do
    case :persistent_term.get(@pt_enabled, :__not_set__) do
      :__not_set__ -> @enable_debug_log
      val -> val
    end
  end

  @doc """
  Emit a debug log if debug logging is enabled.

  Accepts a string, iodata, or a zero-arity anonymous function (lazy
  evaluation).  When disabled, this macro expands to `:ok` so no work
  is performed.

  ## Examples

      require SuperCache.Log

      SuperCache.Log.debug("starting operation")
      SuperCache.Log.debug(fn -> "data: \#{inspect(large_struct)}" end)
  """
  defmacro debug(chardata_or_fun) do
    if @enable_debug_log do
      quote do
        if SuperCache.Log.enabled?() do
          Logger.debug(unquote(chardata_or_fun))
        end
      end
    else
      # Compile-time elimination — expands to :ok with zero runtime overhead.
      # No function call, no string building, no branch prediction miss.
      quote do: :ok
    end
  end

  @doc """
  Emit an info log.

  Unlike `debug/1`, this always logs (info is not gated by the debug
  toggle), but it still accepts lazy functions for consistency.

  ## Examples

      SuperCache.Log.info("cache started with 8 partitions")
  """
  defmacro info(chardata_or_fun) do
    quote do
      Logger.info(unquote(chardata_or_fun))
    end
  end

  @doc """
  Emit a warning log.

  Always logs — not gated by the debug toggle.

  ## Examples

      SuperCache.Log.warning("node not reachable, will retry")
  """
  defmacro warning(chardata_or_fun) do
    quote do
      Logger.warning(unquote(chardata_or_fun))
    end
  end

  @doc """
  Emit an error log.

  Always logs — not gated by the debug toggle.

  ## Examples

      SuperCache.Log.error("3PC commit failed: timeout")
  """
  defmacro error(chardata_or_fun) do
    quote do
      Logger.error(unquote(chardata_or_fun))
    end
  end
end
