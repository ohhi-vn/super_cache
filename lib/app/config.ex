defmodule SuperCache.Config do
  @moduledoc """
  Central configuration store for SuperCache.

  Acts as a GenServer-backed key-value store with an optimisation for
  hot-path keys: values for `@fast_keys` are also written to `:persistent_term`
  so reads are allocation-free (no GenServer hop).

  ## Fast keys

  The following keys are served from `:persistent_term` on every read:

  - `:key_pos` — tuple index used as the ETS key
  - `:partition_pos` — tuple index used to select partition
  - `:num_partition` — number of ETS partitions
  - `:table_type` — ETS table type (`:set`, `:bag`, etc.)
  - `:table_prefix` — prefix for ETS table atom names

  All other keys fall back to a `GenServer.call/3` with a 5-second timeout.

  ## Example

      SuperCache.Config.set_config(:my_key, "value")
      SuperCache.Config.get_config(:my_key)
      # => "value"

      # Fast key — zero-cost read
      SuperCache.Config.get_config(:num_partition)
      # => 8
  """

  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger
  require SuperCache.Log

  defexception message: "incorrect config"

  alias __MODULE__

  # Keys promoted to persistent_term for zero-cost reads on the hot path.
  # Stored as a plain map so `is_map_key/2` works in guard clauses.
  # `:cluster` is included here because `distributed?()` is called on every
  # API operation and must avoid the GenServer hop.
  @fast_keys %{
    cluster: true,
    key_pos: true,
    partition_pos: true,
    num_partition: true,
    table_type: true,
    table_prefix: true
  }

  ## API ##

  @doc """
  Starts the Config GenServer.

  Accepts a keyword list of initial configuration values.
  """
  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Extract the key element from a data tuple using the configured `:key_pos`.

  Raises `Config` if the tuple is too small.

  ## Examples

      SuperCache.Config.set_config(:key_pos, 0)
      SuperCache.Config.get_key!({:user, 1, "Alice"})
      # => :user
  """
  @spec get_key!(tuple) :: any
  def get_key!(data) when is_tuple(data) do
    pos = fast_get(:key_pos)

    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Config,
        message:
          "tuple size (#{tuple_size(data)}) is lower than key_pos (#{pos}) for data: #{inspect(data)}"
    end
  end

  @doc """
  Extract the partition element from a data tuple using the configured `:partition_pos`.

  Raises `Config` if the tuple is too small.

  ## Examples

      SuperCache.Config.set_config(:partition_pos, 1)
      SuperCache.Config.get_partition!({:user, 1, "Alice"})
      # => 1
  """
  @spec get_partition!(tuple) :: any
  def get_partition!(data) when is_tuple(data) do
    pos = fast_get(:partition_pos)

    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Config,
        message:
          "tuple size (#{tuple_size(data)}) is lower than partition_pos (#{pos}) for data: #{inspect(data)}"
    end
  end

  @doc """
  Read a configuration value.

  Hot keys (`@fast_keys`) are served from `:persistent_term` with no
  GenServer hop. All other keys use `GenServer.call/3` with a 5-second
  timeout.

  ## Examples

      SuperCache.Config.get_config(:num_partition)
      # => 8

      SuperCache.Config.get_config(:unknown_key, :default)
      # => :default
  """
  @spec get_config(any, any) :: any
  def get_config(key, default \\ nil) do
    if is_map_key(@fast_keys, key) do
      case :persistent_term.get({__MODULE__, key}, :__not_set__) do
        :__not_set__ -> default
        value -> value
      end
    else
      GenServer.call(__MODULE__, {:get, key, default}, 5_000)
    end
  end

  @doc """
  Check whether a configuration key exists in the GenServer state.

  Note: This always hits the GenServer, even for fast keys, because
  `:persistent_term` does not distinguish between "not set" and "set to nil".
  """
  @spec has_config?(any) :: boolean
  def has_config?(key) do
    GenServer.call(__MODULE__, {:has_config, key}, 5_000)
  end

  @doc """
  Store a configuration value.

  Hot keys are also written to `:persistent_term` for fast subsequent reads.
  """
  @spec set_config(any, any) :: :ok
  def set_config(key, value) do
    GenServer.call(__MODULE__, {:set, key, value}, 5_000)
  end

  @doc """
  Delete a configuration value from the GenServer state and `:persistent_term`.

  Uses a synchronous call to ensure the deletion is complete before returning,
  preventing race conditions with concurrent reads.
  """
  @spec delete_config(any) :: :ok
  def delete_config(key) do
    GenServer.call(__MODULE__, {:delete, key}, 5_000)
  end

  @doc """
  Clear all configuration values and erase all `:persistent_term` entries
  for hot keys.
  """
  @spec clear_config() :: :ok
  def clear_config() do
    GenServer.call(__MODULE__, :clear, 5_000)
  end

  ## GenServer callbacks ##

  @impl true
  def init(opts) do
    SuperCache.Log.debug(fn ->
      "super_cache, config, starting with opts: #{inspect(opts)}"
    end)

    state =
      Enum.reduce(opts, %{}, fn {k, v}, acc -> Map.put(acc, k, v) end)
      |> Map.put_new(:table_prefix, "SuperCache.Storage.Ets")

    # Promote initial values to persistent_term.
    Enum.each(state, fn {k, v} -> maybe_write_fast(k, v) end)

    Logger.info("super_cache, config, started with #{map_size(state)} key(s)")
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
    SuperCache.Log.debug(fn -> "super_cache, config, set #{inspect(key)} = #{inspect(value)}" end)
    {:reply, :ok, Map.put(state, key, value)}
  end

  def handle_call({:delete, key}, _from, state) do
    :persistent_term.erase({__MODULE__, key})
    SuperCache.Log.debug(fn -> "super_cache, config, deleted #{inspect(key)}" end)
    {:reply, :ok, Map.delete(state, key)}
  end

  def handle_call(:clear, _from, state) do
    count = map_size(state)
    Enum.each(Map.keys(@fast_keys), fn k -> :persistent_term.erase({__MODULE__, k}) end)
    SuperCache.Log.debug(fn -> "super_cache, config, cleared #{count} key(s)" end)
    {:reply, :ok, %{}}
  end

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    count = if state, do: map_size(state), else: 0

    Logger.info(
      "super_cache, config, shutting down (#{count} key(s) in state, reason: #{inspect(reason)})"
    )

    :ok
  end

  ## Private helpers ##

  # Bypasses GenServer — safe because persistent_term is read-optimised and
  # updates are rare (only during start!/stop!).
  @compile {:inline, fast_get: 1}
  defp fast_get(key) do
    :persistent_term.get({__MODULE__, key})
  end

  defp maybe_write_fast(key, value) when is_map_key(@fast_keys, key) do
    :persistent_term.put({__MODULE__, key}, value)
  end

  defp maybe_write_fast(_key, _value), do: :ok
end
