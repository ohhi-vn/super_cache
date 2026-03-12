defmodule SuperCache.Distributed.Struct do
  @moduledoc "Compatibility shim → use `SuperCache.Struct` directly."
  @deprecated "Use SuperCache directly — it now handles both modes automatically."
  defdelegate init(struct, key \\ :id),                 to: SuperCache.Struct
  defdelegate add(struct),                              to: SuperCache.Struct
  defdelegate get(struct, opts \\ []),                  to: SuperCache.Struct
  defdelegate get_all(struct, opts \\ []),              to: SuperCache.Struct
  defdelegate remove(struct),                           to: SuperCache.Struct
  defdelegate remove_all(struct),                       to: SuperCache.Struct
end
