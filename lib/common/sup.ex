defmodule SuperCache.Sup do
  @moduledoc """
  DynamicSupervisor, uses for add worker in runtime.
  """

  use DynamicSupervisor
  require Logger

  ## API ###

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def start_worker(workers) do
    Enum.each(workers, fn spec ->
      # TO-DO: Add code handle pid.
      DynamicSupervisor.start_child(__MODULE__, spec)
    end)
  end

  ## Callback ##

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  ## Private function ##

end
