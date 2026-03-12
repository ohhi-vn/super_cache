# =============================================================================
# lib/super_cache/distributed.ex
#
# Backwards-compatibility shim.  Delegates all calls to the unified modules.
# Existing code using SuperCache.Distributed continues to work without change.
# =============================================================================
defmodule SuperCache.Distributed do
  @moduledoc """
  Backwards-compatibility shim for `SuperCache`.

  `SuperCache.Distributed` is now an alias for `SuperCache`.
  Use `SuperCache` directly in new code — it handles both local and
  distributed modes automatically based on the `:cluster` option.

  This module will be removed in a future release.
  """

  @deprecated "Use SuperCache directly — it now handles both modes automatically."
  defdelegate put!(data),                             to: SuperCache
  defdelegate put(data),                              to: SuperCache
  defdelegate lazy_put(data),                         to: SuperCache
  defdelegate get!(data, opts \\ []),                 to: SuperCache
  defdelegate get(data, opts \\ []),                  to: SuperCache
  defdelegate get_by_key_partition!(key, pd, opts \\ []), to: SuperCache
  defdelegate get_by_key_partition(key, pd, opts \\ []),  to: SuperCache
  defdelegate get_same_key_partition!(key, opts \\ []),   to: SuperCache
  defdelegate get_same_key_partition(key, opts \\ []),    to: SuperCache
  defdelegate get_by_match!(pd, pattern, opts \\ []), to: SuperCache
  defdelegate get_by_match!(pattern),                 to: SuperCache
  defdelegate get_by_match(pd, pattern, opts \\ []),  to: SuperCache
  defdelegate get_by_match_object!(pd, pattern, opts \\ []), to: SuperCache
  defdelegate get_by_match_object!(pattern),          to: SuperCache
  defdelegate get_by_match_object(pd, pattern, opts \\ []),  to: SuperCache
  defdelegate scan!(pd, fun, acc),                    to: SuperCache
  defdelegate scan!(fun, acc),                        to: SuperCache
  defdelegate scan(pd, fun, acc),                     to: SuperCache
  defdelegate delete!(data),                          to: SuperCache
  defdelegate delete(data),                           to: SuperCache
  defdelegate delete_all(),                           to: SuperCache
  defdelegate delete_by_match!(pd, pattern),          to: SuperCache
  defdelegate delete_by_match!(pattern),              to: SuperCache
  defdelegate delete_by_match(pd, pattern),           to: SuperCache
  defdelegate delete_by_key_partition!(key, pd),      to: SuperCache
  defdelegate delete_by_key_partition(key, pd),       to: SuperCache
  defdelegate delete_same_key_partition!(key),        to: SuperCache
  defdelegate delete_same_key_partition(key),         to: SuperCache
  defdelegate started?(),                             to: SuperCache
  defdelegate stats(),                                to: SuperCache
  defdelegate cluster_stats(),                        to: SuperCache
end
