# SuperCache - Local Mode Example
#
# This example demonstrates how to use SuperCache in local (single-node) mode.
# Run with: mix run examples/local_mode_example.exs
#
# Note: Struct definitions are in examples/support/example_structs.ex
# and are compiled automatically when running with mix run.

alias SuperCache.{KeyValue, Queue, Stack, Struct}
alias ExampleStructs.User

# ── 1. Start SuperCache ───────────────────────────────────────────────────────

IO.puts("=== Starting SuperCache (Local Mode) ===\n")

# Start with default options (local mode, num_partition = scheduler count)
SuperCache.start!()

IO.puts("SuperCache started successfully!")
IO.puts("Distributed mode? #{SuperCache.distributed?()}")
IO.puts("")

# ── 2. Tuple Storage (Main API) ───────────────────────────────────────────────

IO.puts("=== Tuple Storage ===\n")

# Insert tuples
SuperCache.put!({:user, 1, "Alice"})
SuperCache.put!({:user, 2, "Bob"})
SuperCache.put!({:user, 3, "Charlie"})

IO.puts("Inserted 3 user tuples")

# Retrieve by key (key_pos defaults to 0, so {:user, 1} is the key)
result = SuperCache.get!({:user, 1})
IO.puts("Get user 1: #{inspect(result)}")

# Batch insert (10-100x faster for bulk operations)
batch_data =
  for i <- 4..10 do
    {:user, i, "User_#{i}"}
  end

SuperCache.put_batch!(batch_data)
IO.puts("Batch inserted 7 more users (total: 10)")

# Pattern matching
matches = SuperCache.get_by_match!({:user, :_, :_})
IO.puts("All users matching {:user, :_, :_}: #{length(matches)} records")

# Delete
SuperCache.delete!({:user, 10})
IO.puts("Deleted user 10")

# Stats
stats = SuperCache.stats()
IO.puts("Cache stats: #{inspect(stats)}")
IO.puts("")

# ── 3. Key-Value API ──────────────────────────────────────────────────────────

IO.puts("=== Key-Value API ===\n")

# Add key-value pairs
KeyValue.add("session", :user_1, %{name: "Alice", role: :admin})
KeyValue.add("session", :user_2, %{name: "Bob", role: :user})
KeyValue.add("session", :user_3, %{name: "Charlie", role: :user})

IO.puts("Added 3 sessions")

# Retrieve
session = KeyValue.get("session", :user_1)
IO.puts("Session user_1: #{inspect(session)}")

# Batch operations
KeyValue.add_batch("session", [
  {:user_4, %{name: "Diana", role: :moderator}},
  {:user_5, %{name: "Eve", role: :user}}
])

IO.puts("Batch added 2 more sessions")

# Collection operations
IO.puts("Session keys: #{inspect(KeyValue.keys("session"))}")
IO.puts("Session count: #{KeyValue.count("session")}")

# Remove
KeyValue.remove("session", :user_1)
IO.puts("Removed user_1 session")
IO.puts("Session count after removal: #{KeyValue.count("session")}")

# Clear namespace
KeyValue.remove_all("session")
IO.puts("Cleared all sessions")
IO.puts("Session count after clear: #{KeyValue.count("session")}")
IO.puts("")

# ── 4. Queue (FIFO) ───────────────────────────────────────────────────────────

IO.puts("=== Queue (FIFO) ===\n")

# Add items to queue
Queue.add("jobs", "process_order_1")
Queue.add("jobs", "process_order_2")
Queue.add("jobs", "process_order_3")

IO.puts("Added 3 jobs to queue")
IO.puts("Queue count: #{Queue.count("jobs")}")

# Peek at next item (without removing)
IO.puts("Next job (peak): #{Queue.peak("jobs")}")

# Dequeue items (FIFO order)
job1 = Queue.out("jobs")
job2 = Queue.out("jobs")
IO.puts("Dequeued: #{job1}, #{job2}")
IO.puts("Queue count after dequeue: #{Queue.count("jobs")}")

# Get all remaining
IO.puts("Remaining jobs: #{inspect(Queue.get_all("jobs"))}")
IO.puts("")

# ── 5. Stack (LIFO) ───────────────────────────────────────────────────────────

IO.puts("=== Stack (LIFO) ===\n")

# Push items onto stack
Stack.push("history", "page_a")
Stack.push("history", "page_b")
Stack.push("history", "page_c")

IO.puts("Pushed 3 pages onto stack")
IO.puts("Stack count: #{Stack.count("history")}")

# Pop items (LIFO order)
page1 = Stack.pop("history")
page2 = Stack.pop("history")
IO.puts("Popped: #{page1}, #{page2}")
IO.puts("Stack count after pop: #{Stack.count("history")}")

# Get all remaining
IO.puts("Remaining pages: #{inspect(Stack.get_all("history"))}")
IO.puts("")

# ── 6. Struct Storage ─────────────────────────────────────────────────────────

IO.puts("=== Struct Storage ===\n")

# Initialize struct storage with :id as the key field
Struct.init(%User{}, :id)
IO.puts("Initialized Struct storage for %User{} with key field :id")

# Add structs
Struct.add(%User{id: 1, name: "Alice", email: "alice@example.com"})
Struct.add(%User{id: 2, name: "Bob", email: "bob@example.com"})
Struct.add(%User{id: 3, name: "Charlie", email: "charlie@example.com"})

IO.puts("Added 3 users")

# Retrieve by struct
{:ok, user} = Struct.get(%User{id: 1})
IO.puts("Get user 1: #{inspect(user)}")

# Get all users
{:ok, all_users} = Struct.get_all(%User{})
IO.puts("All users: #{length(all_users)} records")

# Remove
Struct.remove(%User{id: 3})
IO.puts("Removed user 3")
{:ok, remaining} = Struct.get_all(%User{})
IO.puts("Users after removal: #{length(remaining)}")

# Clear all
Struct.remove_all(%User{})
IO.puts("Cleared all users")
{:ok, cleared} = Struct.get_all(%User{})
IO.puts("Users after clear: #{length(cleared)}")
IO.puts("")

# ── 7. Cleanup ────────────────────────────────────────────────────────────────

IO.puts("=== Cleanup ===\n")

# Clear all data
SuperCache.delete_all()
IO.puts("Cleared all cache data")

# Stop SuperCache
SuperCache.stop()
IO.puts("SuperCache stopped")

IO.puts("\n=== Example Complete ===")
