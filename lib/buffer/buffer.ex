defmodule SuperCache.Buffer do
  @moduledoc """
  Manages per-scheduler write buffers used by `SuperCache.lazy_put/1`.

  One buffer process is started per online scheduler so that producers can
  enqueue without cross-scheduler contention.  The buffer atom table is built
  once and stored in `:persistent_term`, making hot-path lookups allocation-free.
  """

  require Logger
  require SuperCache.Log

  alias SuperCache.Internal.Queue, as: LibQueue
  alias SuperCache.Internal.Stream, as: LibStream

  @pt_key {__MODULE__, :buffer_names}

  ## API ##

  @doc """
  Start one buffer stream per scheduler and register their names in
  `:persistent_term`.  Called by `SuperCache.Bootstrap` during `start!/1`.
  """
  @spec start(pos_integer) :: :ok
  def start(num_schedulers) when is_integer(num_schedulers) and num_schedulers > 0 do
    names =
      for id <- 1..num_schedulers do
        name = buffer_atom(id)
        spawn(fn -> start_stream(name) end)
        name
      end

    # Store as a tuple for O(1) indexed access.
    :persistent_term.put(@pt_key, List.to_tuple(names))
    :ok
  end

  @doc """
  Stop all buffer processes and remove the persistent_term entry.
  Called by `SuperCache.Bootstrap` during `stop/0`.
  """
  @spec stop() :: :ok
  def stop() do
    case :persistent_term.get(@pt_key, nil) do
      nil ->
        :ok

      names_tuple ->
        names_tuple
        |> Tuple.to_list()
        |> Enum.each(fn name ->
          case Process.whereis(name) do
            nil -> :ok
            pid -> LibQueue.stop(pid)
          end
        end)

        :persistent_term.erase(@pt_key)
        :ok
    end
  end

  @doc """
  Enqueue `data` into the buffer for the current scheduler.
  Uses `:erlang.system_info(:scheduler_id)` so the call stays on the same
  scheduler without random overhead or atom allocation.
  """
  @spec enqueue(tuple) :: :ok
  def enqueue(data) when is_tuple(data) do
    names = :persistent_term.get(@pt_key)
    # scheduler_id is 1-based; wrap with rem for safety if count changes.
    idx = rem(:erlang.system_info(:scheduler_id) - 1, tuple_size(names))
    LibQueue.add(elem(names, idx), data)
  end

  ## Private ##

  # Atoms are built at startup — never at runtime — so the atom table is safe.
  defp buffer_atom(id), do: String.to_atom("SuperCache.Buffer_#{id}")

  defp start_stream(name) do
    SuperCache.Log.debug(fn -> "super_cache, buffer, starting stream #{inspect(name)}" end)

    name
    |> LibQueue.start()
    |> LibStream.create()
    |> LibStream.make_stream_pipe()

    SuperCache.Log.debug(fn -> "super_cache, buffer, stream #{inspect(name)} finished" end)
  end
end
