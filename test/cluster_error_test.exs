defmodule SuperCache.Cluster.ErrorTest do
  use ExUnit.Case, async: true

  alias SuperCache.Cluster.Error

  describe "struct fields" do
    test "populates every documented field" do
      error = %Error{
        operation: :put,
        partition: 3,
        node: :"cache@10.0.0.2",
        mode: :strong,
        reason: :nodedown
      }

      assert error.operation == :put
      assert error.partition == 3
      assert error.node == :"cache@10.0.0.2"
      assert error.mode == :strong
      assert error.reason == :nodedown
    end

    test "defaults optional fields to nil" do
      error = %Error{reason: :timeout}

      assert error.operation == nil
      assert error.partition == nil
      assert error.node == nil
      assert error.mode == nil
      assert error.message == nil
    end
  end

  describe "message formatting" do
    test "includes operation, partition, node, mode, and reason" do
      error = %Error{
        operation: :put,
        partition: 3,
        node: :"cache@10.0.0.2",
        mode: :strong,
        reason: :nodedown
      }

      message = Error.message(error)

      assert message =~ "put"
      assert message =~ "partition 3"
      assert message =~ "cache@10.0.0.2"
      assert message =~ ":strong"
      assert message =~ "nodedown"
    end

    test "omits unknown context fields" do
      message = Error.message(%Error{reason: :timeout})

      assert message == "distributed operation failed: timeout"
    end

    test "formats module/function operations" do
      error = %Error{operation: {SuperCache.Cluster.Router, :route_put!}, reason: :noconnection}

      assert Error.message(error) =~ "SuperCache.Cluster.Router.route_put!"
    end

    test "formats MFA operations" do
      error = %Error{operation: {SuperCache.Cluster.Router, :route_put!, 2}, reason: :timeout}

      assert Error.message(error) =~ "SuperCache.Cluster.Router.route_put!/2"
    end

    test "formats remote exception reasons with the exception message" do
      error = %Error{
        operation: :get,
        reason: {:exception, :error, %RuntimeError{message: "remote boom"}, []}
      }

      message = Error.message(error)

      assert message =~ "remote boom"
      assert message =~ "RuntimeError"
    end

    test "formats exit and throw reasons" do
      assert Error.message(%Error{reason: {:exit, :boom}}) =~ "exit :boom"
      assert Error.message(%Error{reason: {:throw, :boom}}) =~ "throw :boom"
    end

    test "uses an explicit message when supplied" do
      error = %Error{reason: :timeout, message: "custom failure"}

      assert Error.message(error) == "custom failure"
    end

    test "builds the default exception through exception/1" do
      assert %Error{reason: :timeout, operation: nil, message: nil} =
               Error.exception(reason: :timeout)

      assert %Error{reason: :nodedown} = Error.exception(reason: :nodedown)
    end

    test "inspects a non-integer partition" do
      message = Error.message(%Error{partition: "partition-a", reason: :timeout})

      assert message =~ ~s(partition "partition-a")
    end

    test "inspects an operation that is neither atom nor module/function" do
      message = Error.message(%Error{operation: {:custom, :op, :bad_arity}, reason: :timeout})

      assert message =~ ":custom"
      assert message =~ ":op"
    end

    test "formats an exception struct reason with its message" do
      message = Error.message(%Error{reason: %RuntimeError{message: "local boom"}})

      assert message == "distributed operation failed: local boom"
    end

    test "passes a binary reason through unchanged" do
      assert Error.message(%Error{reason: "disk full"}) ==
               "distributed operation failed: disk full"
    end

    test "inspects a generic term reason" do
      assert Error.message(%Error{reason: {:custom, :failure}}) ==
               "distributed operation failed: {:custom, :failure}"
    end
  end

  describe "Exception.message/1" do
    test "delegates to the module message callback" do
      error = %Error{
        operation: :get,
        partition: 1,
        node: :"cache@10.0.0.3",
        mode: :quorum,
        reason: :timeout
      }

      assert Exception.message(error) == Error.message(error)
      assert Exception.message(error) =~ "get"
      assert Exception.message(error) =~ "partition 1"
      assert Exception.message(error) =~ "quorum"
      assert Exception.message(error) =~ "timeout"
    end

    test "can be raised and rescued as a normal exception" do
      assert_raise Error, "custom failure", fn ->
        raise %Error{reason: :timeout, message: "custom failure"}
      end
    end
  end
end
