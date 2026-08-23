ExUnit.start()

# Exercise the debug-logging path itself: SuperCache.Log starts fully enabled
# so every internal `Log.debug(fun)` closure executes under coverage and the
# compile-time/runtime toggle machinery is tested in situ.
SuperCache.Log.enable(true)
