defmodule SuperCache.LogTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Logger

  describe "runtime toggle" do
    test "enable/1 toggles enabled?/0" do
      SuperCache.Log.enable(false)
      assert SuperCache.Log.enabled?() == false

      SuperCache.Log.enable(true)
      assert SuperCache.Log.enabled?() == true
    end

    test "enable rejects non-booleans" do
      assert_raise FunctionClauseError, fn -> SuperCache.Log.enable("yes") end
    end
  end

  describe "macros" do
    setup do
      SuperCache.Log.enable(true)

      # test.exs caps the logger at :warning; temporarily open it up so
      # debug/info output can be captured.
      orig_level = Logger.level()
      Logger.configure(level: :debug)

      on_exit(fn ->
        # test_helper.exs starts the suite with logging enabled so that every
        # internal Log.debug closure executes under coverage. Restore that
        # state instead of leaving the toggle off for later test files.
        SuperCache.Log.enable(true)
        Logger.configure(level: orig_level)
      end)

      :ok
    end

    test "debug emits when enabled" do
      require SuperCache.Log

      logs =
        capture_log(fn ->
          SuperCache.Log.debug("debug message")
        end)

      assert logs =~ "debug message"
    end

    test "debug accepts lazy functions" do
      require SuperCache.Log

      logs =
        capture_log(fn ->
          SuperCache.Log.debug(fn -> "lazy debug #{inspect({1, 2})}" end)
        end)

      assert logs =~ "lazy debug {1, 2}"
    end

    test "debug is silent when runtime-disabled" do
      require SuperCache.Log
      SuperCache.Log.enable(false)

      logs =
        capture_log(fn ->
          SuperCache.Log.debug("should not appear")
        end)

      refute logs =~ "should not appear"
    end

    test "info/warning/error always emit regardless of debug toggle" do
      require SuperCache.Log
      SuperCache.Log.enable(false)

      logs =
        capture_log(fn ->
          SuperCache.Log.info("info msg")
          SuperCache.Log.warning("warn msg")
          SuperCache.Log.error("error msg")
        end)

      assert logs =~ "warn msg"
      assert logs =~ "error msg"
    end
  end
end
