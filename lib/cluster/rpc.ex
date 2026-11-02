defmodule SuperCache.Cluster.Rpc do
  @moduledoc """
  Thin `:erpc` boundary that normalizes every distributed failure into a
  `SuperCache.Cluster.Error`.

  Call sites should use this module instead of calling `:erpc` directly so
  that node-down, timeout, remote-exception, exit, and throw reasons all
  reach callers in the same shape.

  ## API

  * `call/5` — returns `{:ok, value}` or `{:error, %Error{}}`.
  * `call!/5` — returns `value` or raises `%Error{}`.
  * `normalize/2` — converts a raw reason and context into `%Error{}`.

  ## Context

  The fifth argument (or the second argument to `normalize/2`) is a keyword
  list or map with optional fields:

  * `:operation` — defaults to `{module, function}` from the call.
  * `:partition` — partition index, when the operation targets one.
  * `:node` — defaults to the node being called.
  * `:mode` — replication or read mode active for the operation.
  * `:timeout` — `:erpc` timeout in milliseconds; defaults to `5_000`.

  ## Examples

      Rpc.call(primary, __MODULE__, :local_read, [0, :get, :key],
        partition: 0, mode: :primary)

      # => {:ok, [{:key, "value"}]}

      Rpc.call(unreachable, __MODULE__, :local_read, [0, :get, :key],
        partition: 0, mode: :primary)

      # => {:error, %SuperCache.Cluster.Error{reason: :noconnection, ...}}

      Rpc.call!(primary, __MODULE__, :local_read, [0, :get, :key])
      # raises %SuperCache.Cluster.Error{} when the call fails
  """

  alias SuperCache.Cluster.Error

  @default_timeout 5_000

  @doc """
  Call `module.function(args)` on `node` with a normalized error result.

  The function consolidates `:erpc` exceptions, exits, throws, timeouts,
  and disconnections into `{:error, %SuperCache.Cluster.Error{}}`.  A
  successful remote return of `{:error, reason}` is treated as a value and
  returned as `{:ok, {:error, reason}}`, because it is not a transport
  failure.
  """
  @spec call(node(), module(), atom(), list(), keyword() | map()) ::
          {:ok, term()} | {:error, Error.t()}
  def call(node, module, function, args, context \\ [])
      when is_atom(node) and is_atom(module) and is_atom(function) and is_list(args) do
    context = build_context(node, module, function, context)
    timeout = context_value(context, :timeout, @default_timeout)

    try do
      {:ok, :erpc.call(node, module, function, args, timeout)}
    catch
      :error, {:exception, reason, stack} ->
        {:error, normalize({:exception, :error, reason, stack}, context)}

      :error, reason ->
        {:error, normalize(reason, context)}

      :exit, reason ->
        {:error, normalize({:exit, reason}, context)}

      :throw, reason ->
        {:error, normalize({:throw, reason}, context)}
    end
  end

  @doc """
  Call `module.function(args)` on `node`, raising `%SuperCache.Cluster.Error{}`
  when the call fails.
  """
  @spec call!(node(), module(), atom(), list(), keyword() | map()) :: term()
  def call!(node, module, function, args, context \\ []) do
    case call(node, module, function, args, context) do
      {:ok, value} -> value
      {:error, %Error{} = error} -> raise error
    end
  end

  @doc """
  Normalize a raw `:erpc` or process reason into a `SuperCache.Cluster.Error`.

  Recognized reasons include `:nodedown`, `:noconnection`, `:timeout`,
  `{:erpc, reason}`, `{:exception, class, reason, stack}`,
  `{:exception, reason, stack}`, `{:exit, reason}`, `{:throw, reason}`, and
  any non-exception term.

  An existing `%SuperCache.Cluster.Error{}` is returned with any context
  fields that are still `nil` filled in.
  """
  @spec normalize(term(), keyword() | map()) :: Error.t()
  def normalize(%Error{} = error, context) do
    context = normalize_context(context)

    %Error{
      error
      | operation: error.operation || context_value(context, :operation),
        partition: error.partition || context_value(context, :partition),
        node: error.node || context_value(context, :node),
        mode: error.mode || context_value(context, :mode)
    }
  end

  def normalize(reason, context) do
    context = normalize_context(context)

    %Error{
      operation: context_value(context, :operation),
      partition: context_value(context, :partition),
      node: context_value(context, :node),
      mode: context_value(context, :mode),
      reason: normalize_reason(reason)
    }
  end

  ## ── Private ──────────────────────────────────────────────────────────────────

  defp build_context(node, module, function, context) do
    context = normalize_context(context)

    context
    |> Map.put_new(:operation, {module, function})
    |> Map.put_new(:node, node)
  end

  defp normalize_context(context) when is_list(context), do: Map.new(context)
  defp normalize_context(context) when is_map(context), do: context
  defp normalize_context(_context), do: %{}

  defp context_value(context, key, default \\ nil) do
    case Map.fetch(context, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  # :erpc raises `{:erpc, :noconnection}` for an unreachable node and
  # `{:erpc, :timeout}` when the deadline expires.  Unwrap both so callers
  # see the transport reason, not the erpc wrapper.
  defp normalize_reason({:erpc, reason}), do: normalize_reason(reason)

  # `:erpc` re-raises a remote exception as
  # `{:exception, ExceptionOrReason, Stacktrace}` in the caller.
  defp normalize_reason({:exception, reason, stack}) when is_list(stack) do
    {:exception, :error, reason, stack}
  end

  defp normalize_reason({:exception, _class, _reason, _stack} = reason), do: reason

  defp normalize_reason({:exit, _reason} = reason), do: reason
  defp normalize_reason({:throw, _reason} = reason), do: reason
  defp normalize_reason({:error, _reason} = reason), do: reason

  defp normalize_reason(reason), do: reason
end
