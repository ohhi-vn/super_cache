defmodule SuperCache.Cluster.Error do
  @moduledoc """
  Normalized error for distributed SuperCache operations.

  Every failure that crosses a node boundary — an unreachable primary,
  an `:erpc` timeout, a remote exception, or an exit raised while applying
  an operation — is converted into this exception before it reaches a
  caller.  That keeps raw `:exit` signals and transport-specific tuples out
  of application code.

  ## Fields

  | Field       | Meaning                                                        |
  |-------------|----------------------------------------------------------------|
  | `operation` | Operation that failed, usually `{module, function}` or an atom  |
  | `partition` | Partition index the operation targeted, when known              |
  | `node`      | Node the operation was sent to, when known                      |
  | `mode`      | Replication or read mode active for the operation, when known   |
  | `reason`    | Normalized underlying reason (`:nodedown`, `:timeout`, …)       |
  | `message`   | Optional caller-supplied message; overrides the generated one   |

  ## Error contract

  Bang APIs (`put!/1`, `get!/2`, …) raise this exception.  Non-bang APIs
  (`put/1`, `get/2`, …) return `{:error, %SuperCache.Cluster.Error{}}`
  instead of raising or leaking an exit.

  ## Examples

      error = %SuperCache.Cluster.Error{
        operation: :put,
        partition: 3,
        node: :"cache@10.0.0.2",
        mode: :strong,
        reason: :nodedown
      }

      Exception.message(error)
      # => "distributed operation put failed on partition 3 at node :\"cache@10.0.0.2\" (mode :strong): nodedown"
  """

  defexception [:operation, :partition, :node, :mode, :reason, :message]

  @type operation ::
          atom()
          | {module(), atom()}
          | {module(), atom(), arity()}
          | term()

  @type t :: %__MODULE__{
          operation: operation() | nil,
          partition: non_neg_integer() | nil,
          node: node() | nil,
          mode: atom() | nil,
          reason: term(),
          message: String.t() | nil
        }

  @impl true
  def message(%__MODULE__{message: message}) when is_binary(message), do: message

  def message(%__MODULE__{} = error) do
    "distributed operation#{operation_text(error.operation)} failed" <>
      partition_text(error.partition) <>
      node_text(error.node) <>
      mode_text(error.mode) <>
      ": " <> reason_text(error.reason)
  end

  defp operation_text(nil), do: ""
  defp operation_text(operation), do: " " <> format_operation(operation)

  defp partition_text(nil), do: ""

  defp partition_text(partition) when is_integer(partition),
    do: " on partition #{partition}"

  defp partition_text(partition), do: " on partition #{inspect(partition)}"

  defp node_text(nil), do: ""
  defp node_text(node), do: " at node #{inspect(node)}"

  defp mode_text(nil), do: ""
  defp mode_text(mode), do: " (mode #{inspect(mode)})"

  defp format_operation({module, function, arity})
       when is_atom(module) and is_atom(function) and is_integer(arity),
       do: "#{inspect(module)}.#{function}/#{arity}"

  defp format_operation({module, function}) when is_atom(module) and is_atom(function),
    do: "#{inspect(module)}.#{function}"

  defp format_operation(operation) when is_atom(operation), do: Atom.to_string(operation)
  defp format_operation(operation), do: inspect(operation)

  defp reason_text({:exception, class, reason, _stack}) do
    Exception.format_banner(class, reason)
  end

  defp reason_text({:erpc, reason}), do: "erpc " <> reason_text(reason)
  defp reason_text({:exit, reason}), do: "exit " <> inspect(reason)
  defp reason_text({:throw, reason}), do: "throw " <> inspect(reason)
  defp reason_text({:error, reason}), do: reason_text(reason)

  defp reason_text(%{__exception__: true} = exception), do: Exception.message(exception)

  defp reason_text(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_text(reason) when is_binary(reason), do: reason
  defp reason_text(reason), do: inspect(reason)
end
