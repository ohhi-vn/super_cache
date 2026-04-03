defmodule SuperCache.Sup do
  @moduledoc """
  Dynamic supervisor for user-spawned workers in SuperCache.

  This module provides a `DynamicSupervisor` that allows runtime creation
  and management of child processes. It is primarily used for starting
  buffer streams and other dynamically allocated resources.

  ## Example

      # Start a single worker
      {:ok, pid} = SuperCache.Sup.start_worker({MyWorker, []})

      # Start multiple workers
      workers = [{Worker1, []}, {Worker2, []}]
      results = SuperCache.Sup.start_workers(workers)
  """

  use DynamicSupervisor
  require Logger
  require SuperCache.Log

  ## ── Public API ───────────────────────────────────────────────────────────────

  @doc """
  Starts the dynamic supervisor linked to the current process.

  The supervisor is registered under the name `SuperCache.Sup`.
  """
  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @doc """
  Starts a list of child specifications under this supervisor.

  Returns a list of `{:ok, pid}` or `{:error, reason}` tuples corresponding
  to each child specification.

  ## Examples

      SuperCache.Sup.start_workers([{MyWorker, arg1}, {OtherWorker, arg2}])
      # => [{:ok, #PID<0.100.0>}, {:ok, #PID<0.101.0>}]
  """
  @spec start_workers([Supervisor.child_spec() | {module, term} | module]) :: [
          {:ok, pid} | {:error, term}
        ]
  def start_workers(workers) when is_list(workers) do
    Enum.map(workers, fn spec ->
      result = DynamicSupervisor.start_child(__MODULE__, spec)

      SuperCache.Log.debug(fn ->
        "super_cache, supervisor, start_child result for #{inspect(spec)}: #{inspect(result)}"
      end)

      result
    end)
  end

  @doc """
  Starts a single child specification under this supervisor.

  Returns `{:ok, pid}` on success or `{:error, reason}` on failure.
  """
  @spec start_worker(Supervisor.child_spec() | {module, term} | module) ::
          {:ok, pid} | {:error, term}
  def start_worker(spec) do
    result = DynamicSupervisor.start_child(__MODULE__, spec)

    SuperCache.Log.debug(fn ->
      "super_cache, supervisor, start_child result for #{inspect(spec)}: #{inspect(result)}"
    end)

    result
  end

  @doc """
  Terminates a child process identified by `pid` or `name`.
  """
  @spec stop_worker(pid() | atom()) :: :ok | {:error, term}
  def stop_worker(pid_or_name) do
    case DynamicSupervisor.terminate_child(__MODULE__, pid_or_name) do
      :ok ->
        SuperCache.Log.debug(fn ->
          "super_cache, supervisor, stopped child #{inspect(pid_or_name)}"
        end)

        :ok

      {:error, reason} ->
        Logger.warning(
          "super_cache, supervisor, failed to stop child #{inspect(pid_or_name)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  ## ── Callbacks ────────────────────────────────────────────────────────────────

  @impl true
  def init(_init_arg) do
    Logger.info("super_cache, supervisor, initialising dynamic supervisor")
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def terminate(reason, _state) do
    Logger.info("super_cache, supervisor, shutting down (reason: #{inspect(reason)})")
    :ok
  end
end
