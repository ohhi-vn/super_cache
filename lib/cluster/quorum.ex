defmodule SuperCache.Cluster.Quorum do
  @moduledoc """
  Deadline-bound quorum collection for distributed reads and writes.

  `await/4` starts one task per `{node, task_fun}` entry under a dedicated
  `Task.Supervisor`, then uses selective receive to count results as they
  arrive.  It returns as soon as `required` entries report the same value,
  or when the deadline expires.

  ## Semantics

  - **Every return value counts, including `nil`.**  `nil` is a valid
    cached value and can win a quorum.
  - **Each node is counted once.**  Duplicate node entries are rejected
    before any task starts.
  - **Early termination.**  Remaining tasks are killed and their messages
    are flushed when quorum is reached.
  - **Deadline.**  Every remaining task is killed at the deadline; the
    caller never waits per task and never blocks past `timeout_ms`.
  - **Crash isolation.**  Tasks run under `Task.Supervisor.async_nolink/2`,
    so a crashing task is reported as a failed vote and cannot crash the
    caller.
  - **Clean mailbox.**  Result and monitor messages produced by this call
    are consumed before it returns.

  ## Return values

      {:ok, value, voters}
      {:error, reason, results}

  `voters` lists the nodes that reported the winning value, in arrival
  order.  `results` maps each node that completed to `{:ok, value}` or
  `{:error, reason}`; nodes still running at the deadline appear as
  `:timeout`.

  The main error reasons are `:quorum_not_reached` (all tasks finished
  without a majority), `:timeout` (the deadline expired), and
  `{:duplicate_nodes, nodes}`.

  ## Example

      entries = [
        {:primary@host, fn -> read(:primary) end},
        {:replica1@host, fn -> read(:replica1) end},
        {:replica2@host, fn -> read(:replica2) end}
      ]

      Quorum.await(entries, 2, 1_000)
      # => {:ok, [record], [:primary@host, :replica1@host]}
  """

  @type entry :: {node(), (-> term())}
  @type result ::
          {:ok, term(), [node()]}
          | {:error, term(), %{node() => {:ok, term()} | {:error, term()} | :timeout}}

  @doc """
  Collect a quorum from `entries` within `timeout_ms` milliseconds.

  `entries` must be unique `{node, task_fun}` pairs.  `required` is the
  number of matching votes needed for success.  `opts` supports `:pool`,
  the `Task.Supervisor` name; it defaults to the application-managed
  `SuperCache.Cluster.Quorum.Pool`.
  """
  @spec await([entry()], pos_integer(), non_neg_integer(), keyword()) :: result()
  def await(entries, required, timeout_ms, opts \\ []) do
    pool = Keyword.get(opts, :pool, __MODULE__.Pool)

    with :ok <- validate(entries, required, timeout_ms) do
      cond do
        entries == [] ->
          {:error, :quorum_not_reached, %{}}

        required > length(entries) ->
          {:error, :quorum_not_reached, %{}}

        true ->
          pending = start_tasks(entries, pool)
          deadline = System.monotonic_time(:millisecond) + timeout_ms
          await_loop(pending, required, deadline, %{}, %{}, %{})
      end
    end
  end

  ## ── Validation ───────────────────────────────────────────────────────────────

  defp validate(entries, required, timeout_ms) do
    with :ok <- validate_entries(entries),
         :ok <- validate_required(required),
         :ok <- validate_timeout(timeout_ms) do
      validate_unique_nodes(entries)
    end
  end

  defp validate_entries(entries) do
    if is_list(entries) and Enum.all?(entries, &valid_entry?/1) do
      :ok
    else
      {:error, :invalid_entries, %{}}
    end
  end

  defp validate_required(required) do
    if is_integer(required) and required >= 1 do
      :ok
    else
      {:error, :invalid_required, %{}}
    end
  end

  defp validate_timeout(timeout_ms) do
    if is_integer(timeout_ms) and timeout_ms >= 0 do
      :ok
    else
      {:error, :invalid_timeout, %{}}
    end
  end

  defp validate_unique_nodes(entries) do
    nodes = Enum.map(entries, fn {node, _fun} -> node end)

    case nodes -- Enum.uniq(nodes) do
      [] -> :ok
      duplicates -> {:error, {:duplicate_nodes, Enum.uniq(duplicates)}, %{}}
    end
  end

  defp valid_entry?({node, fun}), do: is_atom(node) and is_function(fun, 0)
  defp valid_entry?(_entry), do: false

  ## ── Task lifecycle ───────────────────────────────────────────────────────────

  defp start_tasks(entries, pool) do
    Enum.reduce(entries, %{}, fn {node, fun}, acc ->
      task = Task.Supervisor.async_nolink(pool, fun)
      Map.put(acc, task.ref, {node, task})
    end)
  end

  ## ── Await loop ───────────────────────────────────────────────────────────────

  defp await_loop(pending, required, deadline, counts, voters, results) do
    case quorum_value(counts, required) do
      {:ok, value} ->
        stop_tasks(pending)
        {:ok, value, Enum.reverse(Map.get(voters, value, []))}

      :none ->
        await_next(pending, required, deadline, counts, voters, results)
    end
  end

  defp await_next(pending, _required, _deadline, _counts, _voters, results)
       when map_size(pending) == 0 do
    {:error, :quorum_not_reached, results}
  end

  defp await_next(pending, required, deadline, counts, voters, results) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {ref, value} when is_map_key(pending, ref) ->
        {node, _task} = Map.fetch!(pending, ref)
        Process.demonitor(ref, [:flush])

        counts = Map.update(counts, value, 1, &(&1 + 1))
        voters = Map.update(voters, value, [node], &[node | &1])
        results = Map.put(results, node, {:ok, value})

        await_loop(
          Map.delete(pending, ref),
          required,
          deadline,
          counts,
          voters,
          results
        )

      {:DOWN, ref, :process, _pid, reason} when is_map_key(pending, ref) ->
        {node, _task} = Map.fetch!(pending, ref)
        results = Map.put(results, node, {:error, reason})

        await_loop(
          Map.delete(pending, ref),
          required,
          deadline,
          counts,
          voters,
          results
        )
    after
      remaining ->
        results =
          Enum.reduce(pending, results, fn {_ref, {node, _task}}, acc ->
            Map.put(acc, node, :timeout)
          end)

        stop_tasks(pending)
        {:error, :timeout, results}
    end
  end

  ## ── Helpers ──────────────────────────────────────────────────────────────────

  defp quorum_value(counts, required) do
    Enum.find(counts, fn {_value, count} -> count >= required end)
    |> case do
      {value, _count} -> {:ok, value}
      nil -> :none
    end
  end

  defp stop_tasks(pending) do
    Enum.each(pending, fn {ref, {_node, task}} ->
      Process.exit(task.pid, :kill)
      Process.demonitor(ref, [:flush])
      flush_result(ref)
    end)
  end

  # A task can enqueue its result immediately before the kill signal lands.
  # Consume any such message so the caller's mailbox is unchanged.
  defp flush_result(ref) do
    receive do
      {^ref, _value} -> flush_result(ref)
    after
      0 -> :ok
    end
  end
end
