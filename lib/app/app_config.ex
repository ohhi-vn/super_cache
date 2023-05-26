defmodule SuperCache.Config do
  @moduledoc false

  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger

  defexception message: "incorrect config"

  alias __MODULE__

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the server.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  get key from data. The key info is stored in config.
  """
  @spec get_key!(tuple) :: any
  def get_key!(data) when is_tuple(data) do
    pos = get_config(:key_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
     raise Config, message: "tuple size is lower than key pos"
    end
  end

  @doc """
  Get get partition data (used for get store location) from data. The partition info is stored in config.
  """
  @spec get_partition!(tuple) :: any
  def get_partition!(data) when is_tuple(data) do
    pos = get_config(:partition_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Config, message: "tuple size is lower than partition pos"
    end
  end

  @doc """
  Gets a config.
  """
  @spec get_config(any, any) :: any
  def get_config(key, default \\ nil) do
    GenServer.call(__MODULE__, {:get, key, default}, 1_000)
  end

  @doc """
  Checks a config is existed or not.
  """
  @spec has_config?(any) :: true | false
  def has_config?(key) do
    GenServer.call(__MODULE__, {:has_config, key}, 1_000)
  end

  @doc """
  Stores a config.
  """
  @spec set_config(any, any) :: :ok
  def set_config(key, value) do
    GenServer.cast(__MODULE__, {:set, key, value})
  end

  @doc """
  Deletes a config.
  """
  @spec delete_config(any) :: :ok
  def delete_config(key) do
    GenServer.cast(__MODULE__, {:delete, key})
  end

  @doc """
  Clear all configs.
  """
  @spec clear_config :: :ok
  def clear_config() do
    GenServer.cast(__MODULE__, :clear)
  end

  @impl true
  @spec init(list(tuple)) :: {:ok, map}
  def init(opts) do
    Logger.info("start api process with default config #{inspect opts}")
    state =
      Enum.reduce(opts, %{}, fn ({key, value}, result) ->
        Map.put(result, key, value)
      end)

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key, default}, _from, state) do
    result = Map.get(state, key, default)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:has_config, key}, _from, state) do
    result = Map.has_key?(state, key)
    {:reply, result, state}
  end

  @impl true
  def handle_cast({:set, key, value}, state) do
    {:noreply,  Map.put(state, key, value)}
  end

  @impl true
  def handle_cast({:delete, key}, state) do
    {:noreply,  Map.delete(state, key)}
  end

  @impl true
  def handle_cast(:clear, _state) do
    {:noreply,  %{}}
  end

  @impl true
  def terminate(_reason, _) do
    :stop
  end
end
