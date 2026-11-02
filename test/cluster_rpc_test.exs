defmodule SuperCache.Cluster.RpcTest do
  use ExUnit.Case, async: true

  alias SuperCache.Cluster.{Error, Rpc}

  defmodule Faulty do
    @moduledoc false
    def exit_now, do: exit(:rpc_exit)
    def throw_now, do: throw(:rpc_throw)
  end

  @context [
    operation: :read_primary,
    partition: 4,
    node: :"cache@10.0.0.2",
    mode: :primary
  ]

  describe "call/5" do
    test "returns {:ok, value} for a successful call" do
      assert {:ok, 6} = Rpc.call(node(), Enum, :sum, [[1, 2, 3]], timeout: 1_000)
    end

    test "returns the context on a successful call" do
      assert {:ok, :nonode@nohost} =
               Rpc.call(node(), :erlang, :node, [], operation: :node_name, timeout: 1_000)
    end

    test "returns a normalized timeout error" do
      assert {:error, %Error{reason: :timeout} = error} =
               Rpc.call(node(), Process, :sleep, [1_000], timeout: 10, mode: :strong)

      assert error.operation == {Process, :sleep}
      assert error.node == node()
      assert error.mode == :strong
    end

    test "returns a normalized remote exception" do
      assert {:error, %Error{reason: {:exception, :error, :badarg, stacktrace}} = error} =
               Rpc.call(node(), String, :to_integer, ["not-a-number"], timeout: 1_000)

      assert is_list(stacktrace)
      assert error.operation == {String, :to_integer}
      assert error.node == node()
    end

    test "returns a normalized node-down error" do
      assert {:error, %Error{node: :"missing@127.0.0.1"} = error} =
               Rpc.call(:"missing@127.0.0.1", :erlang, :node, [], timeout: 100)

      assert error.reason in [:nodedown, :noconnection]
    end

    test "call/4 applies the default context and timeout" do
      assert {:ok, 6} = Rpc.call(node(), Enum, :sum, [[1, 2, 3]])
    end

    test "normalizes a remote exit" do
      assert {:error, %Error{reason: {:exit, _}} = error} =
               Rpc.call(node(), Faulty, :exit_now, [])

      assert error.operation == {Faulty, :exit_now}
      assert error.node == node()
    end

    test "normalizes a remote throw" do
      assert {:error, %Error{reason: {:throw, :rpc_throw}} = error} =
               Rpc.call(node(), Faulty, :throw_now, [])

      assert error.operation == {Faulty, :throw_now}
    end
  end

  describe "call!/5" do
    test "returns the value on success" do
      assert 6 = Rpc.call!(node(), Enum, :sum, [[1, 2, 3]], timeout: 1_000)
    end

    test "call!/4 applies the default context" do
      assert 6 = Rpc.call!(node(), Enum, :sum, [[1, 2, 3]])
    end

    test "raises the normalized error on failure" do
      error =
        assert_raise Error, fn ->
          Rpc.call!(node(), String, :to_integer, ["not-a-number"], timeout: 1_000)
        end

      assert error.operation == {String, :to_integer}
      assert error.node == node()
      assert match?({:exception, :error, :badarg, _}, error.reason)
      assert Exception.message(error) =~ "ArgumentError"
    end
  end

  describe "normalize/2" do
    test "converts :nodedown with the full context" do
      error = Rpc.normalize(:nodedown, @context)

      assert %Error{
               operation: :read_primary,
               partition: 4,
               node: :"cache@10.0.0.2",
               mode: :primary,
               reason: :nodedown
             } = error
    end

    test "converts :noconnection" do
      assert %Error{reason: :noconnection} = Rpc.normalize(:noconnection, @context)
    end

    test "converts :timeout" do
      assert %Error{reason: :timeout} = Rpc.normalize(:timeout, @context)
    end

    test "unwraps {:erpc, reason}" do
      assert %Error{reason: :nodedown} = Rpc.normalize({:erpc, :nodedown}, @context)
      assert %Error{reason: :timeout} = Rpc.normalize({:erpc, :timeout}, @context)
    end

    test "converts a four-element remote exception tuple" do
      stacktrace = [{__MODULE__, :test, 1, []}]

      assert %Error{reason: {:exception, :error, %RuntimeError{}, ^stacktrace}} =
               Rpc.normalize(
                 {:exception, :error, %RuntimeError{message: "boom"}, stacktrace},
                 @context
               )
    end

    test "converts the three-element exception tuple produced by :erpc" do
      stacktrace = [{__MODULE__, :test, 1, []}]

      assert %Error{reason: {:exception, :error, %RuntimeError{}, ^stacktrace}} =
               Rpc.normalize({:exception, %RuntimeError{message: "boom"}, stacktrace}, @context)
    end

    test "converts exit reasons" do
      assert %Error{reason: {:exit, :boom}} = Rpc.normalize({:exit, :boom}, @context)

      assert %Error{reason: {:exit, {%RuntimeError{message: "boom"}, []}}} =
               Rpc.normalize({:exit, {%RuntimeError{message: "boom"}, []}}, @context)
    end

    test "converts throw reasons" do
      assert %Error{reason: {:throw, :boom}} = Rpc.normalize({:throw, :boom}, @context)
    end

    test "keeps non-exception reasons" do
      assert %Error{reason: {:error, :quorum_not_reached}} =
               Rpc.normalize({:error, :quorum_not_reached}, @context)

      assert %Error{reason: {:error, :wal_backpressure}} =
               Rpc.normalize({:error, :wal_backpressure}, @context)
    end

    test "preserves an existing error and fills missing context" do
      original = %Error{reason: :nodedown, operation: :put}

      assert %Error{
               operation: :put,
               partition: 4,
               node: :"cache@10.0.0.2",
               mode: :primary,
               reason: :nodedown
             } = Rpc.normalize(original, @context)
    end

    test "accepts a map context" do
      assert %Error{operation: :get, reason: :timeout} =
               Rpc.normalize(:timeout, %{operation: :get})
    end

    test "falls back to empty context for other context types" do
      assert %Error{
               operation: nil,
               partition: nil,
               node: nil,
               mode: nil,
               reason: :timeout
             } = Rpc.normalize(:timeout, nil)
    end
  end
end
