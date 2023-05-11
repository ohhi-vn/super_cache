defmodule SuperCache.Api do
  use GenServer, restart: :permanent, shutdown: 5_000
  require Logger

  defexception message: "incorrect config"

  alias SuperCache.Api

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  @doc """
  Starts the server.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_key!(data) when is_tuple(data) do
    pos = get_config(:key_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
     raise Api, message: "tuple size is lower than key pos"
    end
  end

  def get_partition!(data) when is_tuple(data) do
    pos = get_config(:partition_pos)
    if pos < tuple_size(data) do
      elem(data, pos)
    else
      raise Api, message: "tuple size is lower than partition pos"
    end
  end

  def get_config(key, default \\ nil) do
    GenServer.call(__MODULE__, {:get, key, default}, 1_000)
  end

  def has_config?(key) do
    GenServer.call(__MODULE__, {:has_config, key}, 1_000)
  end

  def set_config(key, value) do
    GenServer.cast(__MODULE__, {:set, key, value})
  end

  def delete_config(key) do
    GenServer.cast(__MODULE__, {:delete, key})
  end

  def clear_config() do
    GenServer.cast(__MODULE__, :clear)
  end

  @impl true
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
