defmodule SuperCache.Config do
  @moduledoc false
  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger
  require SuperCache.Log

  defexception message: "incorrect config"

  alias __MODULE__

  # Keys promoted to persistent_term for zero-cost reads on the hot path.
  @fast_keys [:key_pos, :partition_pos, :num_partition, :table_type, :table_prefix]

  ## API ##

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Extract key element from a data tuple using configured key_pos."
  @spec get_key!(tuple) :: any
  def get_key!(data) when is_tuple(data) do
    pos = fast_get(:key_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Config, message: "tuple size is lower than key_pos"
    end
  end

  @doc "Extract partition element from a data tuple using configured partition_pos."
  @spec get_partition!(tuple) :: any
  def get_partition!(data) when is_tuple(data) do
    pos = fast_get(:partition_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Config, message: "tuple size is lower than partition_pos"
    end
  end

  @doc "Read a config value. Hot keys are served from persistent_term (no GenServer hop)."
  @spec get_config(any, any) :: any
  def get_config(key, default \\ nil) do
    if key in @fast_keys do
      case :persistent_term.get({__MODULE__, key}, :__not_set__) do
        :__not_set__ -> default
        value -> value
      end
    else
      GenServer.call(__MODULE__, {:get, key, default}, 5_000)
    end
  end

  @doc "Check whether a config key exists."
  @spec has_config?(any) :: boolean
  def has_config?(key) do
    GenServer.call(__MODULE__, {:has_config, key}, 5_000)
  end

  @doc "Store a config value. Hot keys are also written to persistent_term."
  @spec set_config(any, any) :: :ok
  def set_config(key, value) do
    GenServer.call(__MODULE__, {:set, key, value}, 5_000)
  end

  @doc "Delete a config value."
  @spec delete_config(any) :: :ok
  def delete_config(key) do
    GenServer.cast(__MODULE__, {:delete, key})
  end

  @doc "Clear all config values (also erases persistent_term entries for hot keys)."
  @spec clear_config() :: :ok
  def clear_config() do
    GenServer.call(__MODULE__, :clear, 5_000)
  end

  ## GenServer callbacks ##

  @impl true
  def init(opts) do
    SuperCache.Log.debug(fn -> "super_cache, config, starting with opts: #{inspect(opts)}" end)

    state =
      Enum.reduce(opts, %{}, fn {k, v}, acc -> Map.put(acc, k, v) end)
      |> Map.put_new(:table_prefix, "SuperCache.Storage.Ets")

    # Promote initial values to persistent_term.
    Enum.each(state, fn {k, v} -> maybe_write_fast(k, v) end)

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key, default}, _from, state) do
    {:reply, Map.get(state, key, default), state}
  end

  def handle_call({:has_config, key}, _from, state) do
    {:reply, Map.has_key?(state, key), state}
  end

  def handle_call({:set, key, value}, _from, state) do
    maybe_write_fast(key, value)
    {:reply, :ok, Map.put(state, key, value)}
  end

  def handle_call(:clear, _from, _state) do
    Enum.each(@fast_keys, fn k ->
      :persistent_term.erase({__MODULE__, k})
    end)
    {:reply, :ok, %{}}
  end

  @impl true
  def handle_cast({:delete, key}, state) do
    :persistent_term.erase({__MODULE__, key})
    {:noreply, Map.delete(state, key)}
  end

  @impl true
  def terminate(_reason, _state), do: :stop

  ## Private helpers ##

  # Bypasses GenServer — safe because persistent_term is read-optimised and
  # updates are rare (only during start!/stop!).
  @compile {:inline, fast_get: 1}
  defp fast_get(key) do
    :persistent_term.get({__MODULE__, key})
  end

  defp maybe_write_fast(key, value) when key in @fast_keys do
    :persistent_term.put({__MODULE__, key}, value)
  end

  defp maybe_write_fast(_key, _value), do: :ok
end
