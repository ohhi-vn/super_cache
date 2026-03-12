
defmodule SuperCache.Distributed.Queue do
  @moduledoc "Compatibility shim → use `SuperCache.Queue` directly."
  @deprecated "Use SuperCache directly — it now handles both modes automatically."
  defdelegate add(queue_name, value),                   to: SuperCache.Queue
  defdelegate out(queue_name, default \\ nil),          to: SuperCache.Queue
  defdelegate peak(queue_name, default \\ nil, opts \\ []), to: SuperCache.Queue
  defdelegate count(queue_name, opts \\ []),            to: SuperCache.Queue
  defdelegate get_all(queue_name),                      to: SuperCache.Queue
end
