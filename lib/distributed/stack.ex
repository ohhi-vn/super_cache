defmodule SuperCache.Distributed.Stack do
  @moduledoc "Compatibility shim → use `SuperCache.Stack` directly."
  @deprecated "Use SuperCache directly — it now handles both modes automatically."
  defdelegate push(stack_name, value),                  to: SuperCache.Stack
  defdelegate pop(stack_name, default \\ nil),          to: SuperCache.Stack
  defdelegate count(stack_name, opts \\ []),            to: SuperCache.Stack
  defdelegate get_all(stack_name),                      to: SuperCache.Stack
end
