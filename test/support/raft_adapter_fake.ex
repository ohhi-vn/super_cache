defmodule SuperCache.Test.RaftAdapterFake do
  @moduledoc """
  Deterministic stand-in for `RaftEx` used by Raft adapter unit tests.

  Responses are installed per test as a map keyed by function name. A value
  may be a plain result, a list of results consumed one per call (the last one
  repeats), or a function receiving the call arguments.
  """

  @key {__MODULE__, :state}

  def install(responses \\ %{}, trace_pid \\ nil) do
    :persistent_term.put(@key, %{
      responses: Map.new(responses || %{}),
      trace: [],
      trace_pid: trace_pid
    })

    :ok
  end

  def uninstall, do: :persistent_term.erase(@key)

  def trace do
    :persistent_term.get(@key, %{trace: []}).trace |> Enum.reverse()
  end

  def derive_names(system),
    do: call(:derive_names, [system], RaftEx.App.System.derive_names(system))

  def start(config), do: system_start(config)
  def system_start(config), do: call(:system_start, [config], {:ok, self()})
  def stop(system), do: system_stop(system)
  def system_stop(system), do: call(:system_stop, [system], :ok)
  def find_leader(cluster_name), do: call(:find_leader, [cluster_name], {:error, :no_leader})
  def members(server, timeout), do: call(:members, [server, timeout], {:error, :noproc})

  def add_member(leader, server_id, timeout),
    do: call(:add_member, [leader, server_id, timeout], {:ok, [], :leader})

  def add_member(leader, server_id, membership, timeout),
    do: call(:add_member, [leader, server_id, membership, timeout], {:ok, [], :leader})

  def promote_member(leader, server_id, timeout),
    do: call(:promote_member, [leader, server_id, timeout], {:ok, [], :leader})

  def demote_member(leader, server_id, timeout),
    do: call(:demote_member, [leader, server_id, timeout], {:ok, [], :leader})

  def transfer_leadership(leader, target, timeout),
    do: call(:transfer_leadership, [leader, target, timeout], :ok)

  def remove_member(leader, server_id, timeout),
    do: call(:remove_member, [leader, server_id, timeout], {:ok, [], :leader})

  def start_server(system, cluster_name, server_id, machine, desired),
    do: call(:start_server, [system, cluster_name, server_id, machine, desired], :ok)

  def stop_server(system, server_id), do: call(:stop_server, [system, server_id], :ok)

  def force_delete_server(system, server_id),
    do: call(:force_delete_server, [system, server_id], :ok)

  def start_cluster(system, cluster_name, machine, desired, timeout),
    do: call(:start_cluster, [system, cluster_name, machine, desired, timeout], {:ok, [], []})

  def start_or_restart_cluster(system, cluster_name, machine, desired, timeout),
    do:
      call(
        :start_or_restart_cluster,
        [system, cluster_name, machine, desired, timeout],
        {:ok, [], []}
      )

  def process_command(server, command, timeout),
    do: call(:process_command, [server, command, timeout], {:ok, :ok, :leader})

  def local_query(server, query, timeout),
    do: call(:local_query, [server, query, timeout], {:ok, :ok, :leader})

  def leader_query(server, query, timeout),
    do: call(:leader_query, [server, query, timeout], {:ok, :ok, :leader})

  def consistent_query(server, query, timeout),
    do: call(:consistent_query, [server, query, timeout], {:ok, :ok, :leader})

  def key_metrics(server, timeout), do: call(:key_metrics, [server, timeout], %{})

  def member_overview(server, timeout), do: call(:member_overview, [server, timeout], %{})

  defp call(name, args, default) do
    state = :persistent_term.get(@key, %{responses: %{}, trace: [], trace_pid: nil})

    entry = {name, args}
    trace = [entry | state.trace]

    if state.trace_pid, do: send(state.trace_pid, {:raft_adapter_call, entry})

    {result, responses} = take_response(state.responses, name, args, default)

    :persistent_term.put(@key, %{state | responses: responses, trace: trace})
    result
  end

  defp take_response(responses, name, args, default) do
    case Map.fetch(responses, name) do
      :error ->
        {default, responses}

      {:ok, values} when is_list(values) ->
        case values do
          [value] -> {resolve(value, args), responses}
          [value | rest] -> {resolve(value, args), Map.put(responses, name, rest)}
          [] -> {default, responses}
        end

      {:ok, value} ->
        {resolve(value, args), responses}
    end
  end

  defp resolve(value, _args) when is_function(value, 0), do: value.()
  defp resolve(value, args) when is_function(value), do: apply(value, args)
  defp resolve(value, _args), do: value
end
