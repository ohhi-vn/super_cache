
defmodule SuperCache.Distributed.KeyValue do
  @moduledoc "Compatibility shim → use `SuperCache.KeyValue` directly."

  @deprecated "Use SuperCache directly — it now handles both modes automatically."
  defdelegate add(kv_name, key, value),               to: SuperCache.KeyValue
  defdelegate get(kv_name, key, default \\ nil, opts \\ []), to: SuperCache.KeyValue
  defdelegate keys(kv_name, opts \\ []),              to: SuperCache.KeyValue
  defdelegate values(kv_name, opts \\ []),            to: SuperCache.KeyValue
  defdelegate count(kv_name, opts \\ []),             to: SuperCache.KeyValue
  defdelegate to_list(kv_name, opts \\ []),           to: SuperCache.KeyValue
  defdelegate remove(kv_name, key),                   to: SuperCache.KeyValue
  defdelegate remove_all(kv_name),                    to: SuperCache.KeyValue
end
