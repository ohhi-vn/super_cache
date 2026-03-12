defmodule SuperCache.Cluster.NodeMonitor do
  @moduledoc """
  Monitors a declared set of nodes and notifies `SuperCache.Cluster.Manager`
  when they join or leave, so partition maps are updated.

  ## Node-source options

  | Option         | Type                     | Description                                          |
  |----------------|--------------------------|------------------------------------------------------|
  | `:nodes`       | `[node()]`               | Static list evaluated once at start-up.              |
  | `:nodes_mfa`   | `{module, atom, [term]}` | Called at init and then every `:refresh_ms`.         |
  | *(none)*       | —                        | Falls back to watching **all** Erlang nodes (legacy).|

  ## Other options

  | Option        | Type    | Default | Description                                               |
  |---------------|---------|---------|-----------------------------------------------------------|
  | `:refresh_ms` | pos_int | `5_000` | Re-evaluation interval for `:nodes_mfa`. Ignored for the |
  |               |         |         | other two sources.                                        |

  ## Source behaviour

  * **`:nodes` (static)** — the managed set is fixed at start-up; no refresh
    timer is scheduled. `:nodeup` / `:nodedown` kernel messages are forwarded
    to `Manager` only for nodes in the set.
  * **`:nodes_mfa` (dynamic)** — the MFA is called at init and on every
    `:refresh` tick. Nodes that appear in the new result are connected and
    announced as up; nodes that disappear are announced as down.
    If the MFA raises an exception **or exits** (e.g. a named process it calls
    has been stopped), the error is logged, an empty list is used, and the
    monitor continues running — it does **not** crash.
  * **`:all` (fallback)** — behaviour identical to the original implementation:
    all `:nodeup` / `:nodedown` kernel events are forwarded unconditionally.
    This is the default when neither `:nodes` nor `:nodes_mfa` is supplied,
    which allows the application supervisor to start the monitor with `[]`
    during normal boot without error.

  ## Runtime reconfiguration

  `reconfigure/1` swaps the source and managed set atomically via a
  `GenServer.call/2` without restarting the process or touching the OTP
  supervisor. Any pending MFA `:refresh` timers from the previous source
  are superseded; stale timer messages that arrive after reconfiguration
  are silently ignored because they carry the old source ref in a tagged
  tuple `{:refresh, ref}`.

  When switching sources the monitor computes the symmetric difference
  between the old and new managed sets and immediately announces:
  - nodes that *appeared* in the new set → `Manager.node_up/1`
  - nodes that *disappeared* from the old set → `Manager.node_down/1`

  ## Examples

      # Started by the application supervisor with no configuration (legacy mode)
      NodeMonitor.start_link([])

      # Static list
      NodeMonitor.start_link(nodes: [:"a@host", :"b@host"])

      # MFA — e.g. read from a config server or service-discovery endpoint
      NodeMonitor.start_link(nodes_mfa: {MyApp.Discovery, :cache_nodes, []})
      NodeMonitor.start_link(nodes_mfa: {MyApp.Discovery, :cache_nodes, []}, refresh_ms: 10_000)

      # Reconfigure at runtime (no supervisor restart needed)
      NodeMonitor.reconfigure(nodes: [:"a@host", :"b@host"])
  """

  use GenServer, restart: :permanent, shutdown: 5_000

  require Logger

  alias SuperCache.Cluster.Manager, as: ClusterManager

  @default_refresh_ms 5_000

  # ── Public API ───────────────────────────────────────────────────────────────

  def start_link(opts) do
    # Validate opts eagerly in the caller's process so ArgumentError propagates
    # as a raised exception rather than being caught by GenServer machinery and
    # returned as {:error, reason}.  Without this, two things swallow the error:
    #
    # 1. GenServer.start_link catches exceptions in init/1 and converts them to
    #    {:error, reason} — assert_raise never sees an exception.
    # 2. If the named process is already registered, start_link returns
    #    {:error, {:already_started, pid}} without calling init at all, so
    #    resolve_source is never reached regardless.
    #
    # Calling resolve_source here raises BEFORE GenServer.start_link is invoked,
    # in the caller's process, where assert_raise can observe it.
    resolve_source(opts)
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Reconfigure the node source at runtime without restarting the process.

  Accepts the same opts as `start_link/1`. Computes the diff between the
  old and new managed sets and immediately calls `Manager.node_up/1` /
  `Manager.node_down/1` for nodes that joined or left the managed set.

  MFA refresh timers from the previous configuration are superseded; any
  stale `{:refresh, ref}` messages that fire after reconfiguration are
  ignored because they carry a ref that no longer matches `state.refresh_ref`.

  Primarily useful in tests that need to switch between static lists or MFA
  sources without touching the OTP supervisor.
  """
  @spec reconfigure(keyword) :: :ok
  def reconfigure(opts), do: GenServer.call(__MODULE__, {:reconfigure, opts}, 10_000)

  # ── GenServer callbacks ──────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    source     = resolve_source(opts)
    refresh_ms = Keyword.get(opts, :refresh_ms, @default_refresh_ms)

    :net_kernel.monitor_nodes(true, node_type: :all)

    {managed, refresh_ref} = activate_source(source, refresh_ms, :all)

    Logger.info(
      "super_cache, node_monitor, started — source: #{inspect(source)}, " <>
        "managed: #{inspect(managed)}"
    )

    {:ok, %{source: source, managed: managed, refresh_ms: refresh_ms, refresh_ref: refresh_ref}}
  end

  # ── Reconfigure ──────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:reconfigure, opts}, _from, state) do
    new_source     = resolve_source(opts)
    new_refresh_ms = Keyword.get(opts, :refresh_ms, @default_refresh_ms)

    # Activate the new source; old refresh timer ref is superseded by the new one.
    {new_managed, new_ref} = activate_source(new_source, new_refresh_ms, state.managed)

    # Step 1 — announce deltas to Manager.
    announce_diff(state.managed, new_managed)

    # Step 2 — reconciliation pass.
    #
    # announce_diff is purely diff-based: it only calls node_up for nodes
    # *entering* the managed set and node_down for nodes *leaving* it.
    # This is insufficient when Manager's internal live_nodes has drifted from
    # NodeMonitor's managed set — for example:
    #
    #   • A health-check retry loop gave up on a node that never actually stopped
    #     (e.g., the node was transiently unreachable, or the erpc timeout raced
    #     with an in-progress Bootstrap.start!/1 on the remote side).
    #   • A node_down/node_up race during a prior reconfigure left Manager without
    #     a node that is now connected and should be live.
    #
    # In both cases, the node stays in `new_managed` across the reconfigure, so
    # announce_diff sees "no change" and calls nothing.  The node is absent from
    # Manager.live_nodes indefinitely.
    #
    # Fix: after the diff, re-announce every node in new_managed that is currently
    # connected at the Erlang level.  Manager.node_up is idempotent for nodes
    # already in live_nodes (the health-check is a no-op if already :started).
    # This does NOT fire during MFA refresh ticks — only on explicit reconfigure
    # calls — so the extra node_up calls are infrequent and harmless.
    reconcile_with_manager(new_managed)

    Logger.info(
      "super_cache, node_monitor, reconfigured — source: #{inspect(new_source)}, " <>
        "managed: #{inspect(new_managed)}"
    )

    new_state = %{
      state
      | source:      new_source,
        managed:     new_managed,
        refresh_ms:  new_refresh_ms,
        refresh_ref: new_ref
    }

    {:reply, :ok, new_state}
  end

  def handle_call(_msg, _from, state), do: {:reply, {:error, :unknown_call}, state}

  # ── Kernel node-monitor events ───────────────────────────────────────────────

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    if watched?(node, state.managed) do
      Logger.info("super_cache, node_monitor, node up: #{inspect(node)}")
      ClusterManager.node_up(node)
    end

    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    if watched?(node, state.managed) do
      Logger.warning("super_cache, node_monitor, node down: #{inspect(node)}")
      ClusterManager.node_down(node)
    end

    {:noreply, state}
  end

  # ── MFA refresh tick ─────────────────────────────────────────────────────────

  # Guard: only act on the refresh tagged with the *current* source ref.
  # Stale timers from a superseded MFA source carry an old ref and are dropped.
  def handle_info({:refresh, ref}, %{refresh_ref: ref, source: source} = state) do
    new_nodes = source |> fetch_nodes() |> connect_all()
    new_set   = MapSet.new(new_nodes)

    announce_diff(state.managed, new_set)

    new_ref = schedule_refresh(state.refresh_ms)
    {:noreply, %{state | managed: new_set, refresh_ref: new_ref}}
  end

  # Stale tagged refresh from a superseded source — silently drop.
  def handle_info({:refresh, _old_ref}, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  # ── Private — source activation ───────────────────────────────────────────────

  # Transitions to `new_source`, returns {new_managed_set, new_refresh_ref}.
  defp activate_source(new_source, refresh_ms, _old_managed) do
    case new_source do
      :all ->
        # Legacy mode: all kernel events forwarded; no managed set, no timer.
        {:all, nil}

      other ->
        nodes   = other |> fetch_nodes() |> connect_all()
        managed = MapSet.new(nodes)
        ref     = if match?({:mfa, _}, other), do: schedule_refresh(refresh_ms), else: nil
        {managed, ref}
    end
  end

  # ── Private — diff announcer ─────────────────────────────────────────────────

  defp announce_diff(:all, :all), do: :ok

  # Transitioning FROM :all mode to a specific managed set.
  #
  # We cannot know what Manager's current live_nodes list looks like — in :all
  # mode, any :nodedown event forwarded by NodeMonitor could have caused Manager
  # to drop a peer.  Announce every node in the new set as up so Manager can
  # run node_running? and re-add any that were silently lost.
  #
  # Manager.node_up is idempotent for nodes that are already live:
  # handle_cast({:node_up, n}) calls Enum.uniq before storing the updated list,
  # so a double-announce has no visible effect other than an extra health-check.
  defp announce_diff(:all, new_set) do
    Enum.each(new_set, fn n ->
      Logger.info("super_cache, node_monitor, re-announcing on :all→set transition: #{inspect(n)}")
      ClusterManager.node_up(n)
    end)
  end

  defp announce_diff(_old, :all), do: :ok

  defp announce_diff(old_set, new_set) do
    new_set
    |> MapSet.difference(old_set)
    |> Enum.each(fn n ->
      Logger.info("super_cache, node_monitor, node added: #{inspect(n)}")
      ClusterManager.node_up(n)
    end)

    old_set
    |> MapSet.difference(new_set)
    |> Enum.each(fn n ->
      Logger.warning("super_cache, node_monitor, node removed: #{inspect(n)}")
      ClusterManager.node_down(n)
    end)
  end

  # Re-announce every node in `managed` that is currently Erlang-connected.
  #
  # Called only from reconfigure/1 — not from the MFA refresh tick — so the
  # extra node_up calls are bounded and infrequent.
  #
  # :all means legacy mode; we have no managed list to reconcile, skip.
  defp reconcile_with_manager(:all), do: :ok

  defp reconcile_with_manager(managed) do
    connected = MapSet.new(Node.list(:connected))

    managed
    |> MapSet.intersection(connected)
    |> Enum.each(fn n ->
      Logger.debug("super_cache, node_monitor, reconcile node_up: #{inspect(n)}")
      ClusterManager.node_up(n)
    end)
  end

  # ── Private — source resolution ───────────────────────────────────────────────

  defp resolve_source(opts) do
    cond do
      nodes = Keyword.get(opts, :nodes) ->
        unless is_list(nodes) and Enum.all?(nodes, &is_atom/1) do
          raise ArgumentError, ":nodes must be a list of node atoms"
        end
        {:static, nodes}

      mfa = Keyword.get(opts, :nodes_mfa) ->
        unless match?({m, f, a} when is_atom(m) and is_atom(f) and is_list(a), mfa) do
          raise ArgumentError, ":nodes_mfa must be a {module, function, args} tuple"
        end
        {:mfa, mfa}

      true ->
        :all
    end
  end

  defp fetch_nodes(:all),             do: []
  defp fetch_nodes({:static, nodes}), do: nodes

  defp fetch_nodes({:mfa, {m, f, a}}) do
    # Use try/rescue/catch to handle BOTH Elixir exceptions AND Erlang exits.
    # An exit (e.g. the named process the MFA calls has been stopped) is NOT
    # caught by `rescue` — only by `catch :exit, _`. Without this guard,
    # a dead named process causes NodeMonitor itself to crash.
    try do
      apply(m, f, a)
    rescue
      err ->
        Logger.error(
          "super_cache, node_monitor, nodes_mfa #{inspect({m, f, a})} raised: #{inspect(err)}"
        )
        []
    catch
      :exit, reason ->
        Logger.error(
          "super_cache, node_monitor, nodes_mfa #{inspect({m, f, a})} exited: #{inspect(reason)}"
        )
        []
    end
  end

  # :all means every kernel event passes the gate.
  defp watched?(_node, :all), do: true
  defp watched?(node, set),   do: MapSet.member?(set, node)

  # Attempt Erlang-level connections; return the list minus self.
  defp connect_all(nodes) do
    me = node()

    nodes
    |> Enum.reject(&(&1 == me))
    |> tap(fn peers ->
      Enum.each(peers, fn n ->
        case Node.connect(n) do
          true ->
            Logger.debug("super_cache, node_monitor, connected to #{inspect(n)}")

          false ->
            Logger.warning(
              "super_cache, node_monitor, could not connect to #{inspect(n)} " <>
                "(will retry on :nodeup)"
            )

          :ignored ->
            Logger.warning(
              "super_cache, node_monitor, node not distributed — #{inspect(n)} ignored"
            )
        end
      end)
    end)
  end

  # Returns a unique ref used to tag and validate refresh timers.
  # This prevents stale timers from a superseded MFA source from
  # triggering a refresh against the new source's managed set.
  defp schedule_refresh(ms) do
    ref = make_ref()
    Process.send_after(self(), {:refresh, ref}, ms)
    ref
  end
end
