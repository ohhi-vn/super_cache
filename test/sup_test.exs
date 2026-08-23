defmodule SuperCache.SupTest do
  use ExUnit.Case, async: false

  defmodule Sleeper do
    use GenServer

    # Deliberately unregistered so multiple instances can run at once.
    def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

    @impl true
    def init(arg), do: {:ok, arg}
  end

  defmodule NamedSleeper do
    use GenServer

    def start_link(arg), do: GenServer.start_link(__MODULE__, arg, name: __MODULE__)

    @impl true
    def init(arg), do: {:ok, arg}
  end

  test "start_worker/1 starts a child and returns its pid" do
    assert {:ok, pid} = SuperCache.Sup.start_worker({Sleeper, []})
    assert Process.alive?(pid)

    on_exit(fn -> SuperCache.Sup.stop_worker(pid) end)
  end

  test "start_workers/1 starts all children preserving result order" do
    results = SuperCache.Sup.start_workers([{Sleeper, []}, {Sleeper, []}])

    assert length(results) == 2
    assert Enum.all?(results, &match?({:ok, _}, &1))

    Enum.each(results, fn {:ok, pid} ->
      assert Process.alive?(pid)
      on_exit(fn -> SuperCache.Sup.stop_worker(pid) end)
    end)
  end

  test "start_worker/1 raises for a nonexistent child module" do
    assert_raise ArgumentError, fn ->
      SuperCache.Sup.start_worker({Nonexistent.Module.XYZ, []})
    end
  end

  test "stop_worker/1 accepts pids" do
    {:ok, pid} = SuperCache.Sup.start_worker({Sleeper, []})
    assert :ok = SuperCache.Sup.stop_worker(pid)
    Process.sleep(20)
    refute Process.alive?(pid)
  end

  test "stop_worker/1 resolves registered atom names" do
    {:ok, _pid} = SuperCache.Sup.start_worker({NamedSleeper, []})
    assert :ok = SuperCache.Sup.stop_worker(NamedSleeper)
  end

  test "stop_worker/1 returns an error for unknown names" do
    assert {:error, :not_found} = SuperCache.Sup.stop_worker(:never_registered_worker_xyz)
  end
end
