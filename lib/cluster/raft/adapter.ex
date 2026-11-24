defmodule SuperCache.Cluster.Raft.Adapter do
  @moduledoc """
  Thin boundary around the experimental `raft_ex` API.

  Production always delegates to `RaftEx`. Tests can set the application env
  `:raft_adapter` to a module exposing the same functions, which makes Raft
  failure and membership branches deterministic without a live cluster.
  """

  @backend_env :raft_adapter
  @default_backend RaftEx

  @doc "Return the configured backend module."
  @spec backend() :: module
  def backend, do: Application.get_env(:super_cache, @backend_env, @default_backend)

  @doc "Delegate to the backend's `RaftEx.App.System.derive_names/1`."
  @spec derive_names(atom) :: map
  def derive_names(system) do
    mod = system_module()
    mod.derive_names(system)
  end

  @doc "Delegate to the backend's `RaftEx.App.System.start/1`."
  @spec system_start(map) :: term
  def system_start(config) do
    mod = system_module()
    mod.start(config)
  end

  @doc "Delegate to the backend's `RaftEx.App.System.stop/1`."
  @spec system_stop(system :: atom) :: term
  def system_stop(system) do
    mod = system_module()
    mod.stop(system)
  end

  defp system_module do
    case backend() do
      RaftEx -> RaftEx.App.System
      other -> other
    end
  end

  @doc "Delegate to the backend's `RaftEx.find_leader/1`."
  @spec find_leader(term) :: term
  def find_leader(cluster_name), do: backend().find_leader(cluster_name)

  @doc "Delegate to the backend's `RaftEx.members/2`."
  @spec members(term, timeout) :: term
  def members(server, timeout), do: backend().members(server, timeout)

  @doc "Delegate to the backend's `RaftEx.add_member/3`."
  @spec add_member(term, term, timeout) :: term
  def add_member(leader, server_id, timeout), do: backend().add_member(leader, server_id, timeout)

  @doc "Delegate to the backend's `RaftEx.remove_member/3`."
  @spec remove_member(term, term, timeout) :: term
  def remove_member(leader, server_id, timeout),
    do: backend().remove_member(leader, server_id, timeout)

  @doc "Delegate to the backend's `RaftEx.start_server/5`."
  @spec start_server(term, term, term, term, [term]) :: term
  def start_server(system, cluster_name, server_id, machine, desired),
    do: backend().start_server(system, cluster_name, server_id, machine, desired)

  @doc "Delegate to the backend's `RaftEx.stop_server/2`."
  @spec stop_server(term, term) :: term
  def stop_server(system, server_id), do: backend().stop_server(system, server_id)

  @doc "Delegate to the backend's `RaftEx.start_cluster/5`."
  @spec start_cluster(term, term, term, [term], timeout) :: term
  def start_cluster(system, cluster_name, machine, desired, timeout),
    do: backend().start_cluster(system, cluster_name, machine, desired, timeout)

  @doc "Delegate to the backend's `RaftEx.start_or_restart_cluster/5`."
  @spec start_or_restart_cluster(term, term, term, [term], timeout) :: term
  def start_or_restart_cluster(system, cluster_name, machine, desired, timeout),
    do: backend().start_or_restart_cluster(system, cluster_name, machine, desired, timeout)

  @doc "Delegate to the backend's `RaftEx.process_command/3`."
  @spec process_command(term, term, timeout) :: term
  def process_command(server, command, timeout),
    do: backend().process_command(server, command, timeout)
end
