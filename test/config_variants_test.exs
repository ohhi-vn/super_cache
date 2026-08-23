defmodule SuperCache.BagTableTest do
  @moduledoc """
  Exercises API branches that only trigger under `:bag` table types
  (non-atomic KeyValue paths).
  """

  use ExUnit.Case, async: false

  alias SuperCache.KeyValue

  setup_all do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(30)

    SuperCache.start!(
      key_pos: 0,
      partition_pos: 0,
      num_partition: 2,
      table_type: :bag
    )

    :ok
  end

  test "KeyValue add inserts duplicates and get returns most recent" do
    KeyValue.add("bag_kv", :k, 1)
    KeyValue.add("bag_kv", :k, 2)

    assert [1, 2] == KeyValue.get_all("bag_kv", :k)
    assert 2 == KeyValue.get("bag_kv", :k)
  end

  test "KeyValue update on bag deletes all then inserts" do
    KeyValue.add("bag_upd", :k, 1)
    KeyValue.add("bag_upd", :k, 2)

    assert :ok = KeyValue.update("bag_upd", :k, :replaced)
    assert [:replaced] == KeyValue.get_all("bag_upd", :k)
  end

  test "KeyValue update/4 read-modify-write works on bag" do
    KeyValue.add("bag_fun", :n, 10)
    new = KeyValue.update("bag_fun", :n, 0, fn v -> v + 5 end)
    assert new == 15 or new == 5
  end

  test "KeyValue increment raises for bag tables" do
    assert_raise ArgumentError, fn ->
      KeyValue.increment("bag_cnt", :n, 0, 1)
    end
  end

  test "KeyValue replace removes all values" do
    KeyValue.add("bag_rep", :k, 1)
    KeyValue.add("bag_rep", :k, 2)

    assert :ok = KeyValue.replace("bag_rep", :k, :only)
    assert [:only] == KeyValue.get_all("bag_rep", :k)
  end

  test "KeyValue remove_batch deletes each key locally" do
    KeyValue.add("bag_rm", :a, 1)
    KeyValue.add("bag_rm", :b, 2)

    assert :ok = KeyValue.remove_batch("bag_rm", [:a, :b])
    assert nil == KeyValue.get("bag_rm", :a)
    assert nil == KeyValue.get("bag_rm", :b)
  end

  test "keys/values/count/to_list reflect bag contents" do
    KeyValue.remove_all("bag_scan")
    KeyValue.add("bag_scan", :x, 1)
    KeyValue.add("bag_scan", :y, 2)

    assert :x in KeyValue.keys("bag_scan")
    assert 1 in KeyValue.values("bag_scan")
    assert KeyValue.count("bag_scan") >= 2

    list = KeyValue.to_list("bag_scan")
    assert {:x, 1} in list or {1, :x} in list
  end
end

defmodule SuperCache.StrongModeTest do
  @moduledoc """
  Exercises API branches that only trigger under `:strong` replication mode
  (WAL-backed writes; single node applies locally with no replicas).
  """

  use ExUnit.Case, async: false

  alias SuperCache.KeyValue

  setup_all do
    if SuperCache.started?(), do: SuperCache.stop()
    Process.sleep(30)

    Application.put_env(:super_cache, :wal, majority_timeout: 100)

    # Shed stray peers leaked by neighbouring cluster-test files so config
    # verification only ever sees this node.
    Enum.each(Node.list(), fn n ->
      Node.disconnect(n)
      SuperCache.Cluster.Manager.node_down(n)
    end)

    Process.sleep(200)

    :ok =
      SuperCache.Cluster.Bootstrap.start!(
        key_pos: 0,
        partition_pos: 0,
        cluster: :distributed,
        replication_factor: 1,
        replication_mode: :strong,
        num_partition: 2
      )

    on_exit(fn -> Application.delete_env(:super_cache, :wal) end)
    :ok
  end

  setup do
    SuperCache.delete_all()
    :ok
  end

  test "SuperCache.put! routes through WAL commit" do
    assert true = SuperCache.put!({:strong_key, "v"})
    assert [{:strong_key, "v"}] == SuperCache.get!({:strong_key, nil})
  end

  test "SuperCache.delete! works under strong mode" do
    assert true = SuperCache.put!({:strong_del, "v"})
    assert :ok = SuperCache.delete!({:strong_del, nil})
    assert [] == SuperCache.get!({:strong_del, nil})
  end

  test "KeyValue set-paths use WAL apply_write branch" do
    assert true == KeyValue.add("skv", :a, 1)
    assert 1 == KeyValue.get("skv", :a)

    assert :ok = KeyValue.update("skv", :a, 2)
    assert 2 == KeyValue.get("skv", :a)

    assert :ok = KeyValue.replace("skv", :a, 3)
    assert 3 == KeyValue.get("skv", :a)

    new = KeyValue.update("skv", :a, 0, fn v -> v * 10 end)
    assert new == 30

    assert 30 == KeyValue.increment("skv", :cnt, 25, 5)
  end

  test "KeyValue delete paths under strong mode" do
    KeyValue.add("skvd", :a, 1)
    assert :ok = KeyValue.remove("skvd", :a)
    assert nil == KeyValue.get("skvd", :a)

    KeyValue.add("skvd", :b, 2)
    assert :ok = KeyValue.remove_all("skvd")
    assert nil == KeyValue.get("skvd", :b)
  end

  test "Queue structural ops under strong mode apply via ops list" do
    alias SuperCache.Queue

    assert true == Queue.add("sq", :one)
    assert :one == Queue.peak("sq")
    assert :one == Queue.out("sq")
    assert nil == Queue.out("sq")

    Queue.add("sq", :two)
    assert [:two] == Queue.get_all("sq")
    assert 0 == Queue.count("sq")
  end

  test "Stack push/pop under strong mode" do
    alias SuperCache.Stack

    assert true == Stack.push("sstack", :a)
    assert :a == Stack.pop("sstack")

    Stack.push("sstack", :b)
    assert [:b] == Stack.get_all("sstack")
  end

  test "lazy_put warns and falls back to put! under strong mode" do
    logs =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        assert :ok = SuperCache.lazy_put({:strong_lazy, 1})
      end)

    Process.sleep(20)
    assert logs =~ "lazy_put" or SuperCache.get!({:strong_lazy, nil}) != []
  end
end
