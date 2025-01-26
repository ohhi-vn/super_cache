defmodule SuperCache.Sup do
  @moduledoc false

  use DynamicSupervisor
  require Logger

  ## API ###

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @spec start_worker(any) :: any
  def start_worker(workers) do
    for spec <- workers do
      # TO-DO: Add code handle pid.
      r = DynamicSupervisor.start_child(__MODULE__, spec)
      Logger.debug("super_cache, supervisor, result for start child (#{inspect spec}): #{inspect r}")
  end
  end

  ## Callback ##

  @impl true
  @spec init(any) ::
          {:ok,
           %{
             extra_arguments: list,
             intensity: non_neg_integer,
             max_children: :infinity | non_neg_integer,
             period: pos_integer,
             strategy: :one_for_one
           }}
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  ## Private function ##

end
