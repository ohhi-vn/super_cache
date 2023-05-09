defmodule SuperCache.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  @impl true
  @spec start(any, any) :: {:error, any} | {:ok, pid}
  def start(_type, _args) do
    Logger.info("startting SuperCache app...")
    children = [
      {SuperCache.Api, [key_pos: 0, partition_pos: 0]},
      {SuperCache.Partition.Common, []},
      {SuperCache.Sup, []},
      {SuperCache.Partition.Holder, []}

    ]

    Logger.info("startting SuperCache with workers: #{inspect children}")
    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Api.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
