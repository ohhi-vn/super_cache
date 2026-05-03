defmodule SuperCache.Storage.MatchSpec do
  @moduledoc """
  Compiled match specifications for ETS queries.

  This module provides helper functions to create and compile match specs
  for common ETS query patterns. Compiled match specs can be reused across
  multiple queries for better performance.

  ## Why use match specs?

  - `:ets.match/2` and `:ets.match_object/2` parse the pattern every time
  - `:ets.select/2` with a compiled match spec skips parsing
  - For repeated queries, compiled specs reduce CPU overhead

  ## Example

      # Instead of:
      :ets.match(table, {:"$1", :"$2"})

      # Use:
      spec = MatchSpec.match([{:"$1", :"$2"}], [:"$1", :"$2"])
      :ets.select(table, spec)
  """

  @type match_spec :: [{tuple, [tuple], [tuple]}]

  @doc """
  Create a match spec for `:ets.match/2` style queries.

  Returns a match spec that, when used with `:ets.select/2`, returns
  a list of binding lists (like `:ets.match/2`).

  ## Parameters

    - `pattern`: The match pattern with `:"$1"`, `:"$2"`, etc.
    - `bindings`: List of bindings to return (e.g., `[:"$1", :"$2"]`)

  ## Example

      # Match all records and return the first two fields
      spec = MatchSpec.match({:"$1", :"$2", :_}, [:"$1", :"$2"])
      :ets.select(table, spec)
  """
  @spec match(tuple | atom, [term]) :: match_spec
  def match(pattern, bindings) when is_list(bindings) do
    # Wrap bindings in a list so :ets.select returns [[binding1, binding2]] like :ets.match
    [{pattern, [], [bindings]}]
  end

  @doc """
  Create a match spec for `:ets.match_object/2` style queries.

  Returns a match spec that, when used with `:ets.select/2`, returns
  full matching tuples (like `:ets.match_object/2`).

  ## Parameters

    - `pattern`: The match pattern with `:"$1"`, `:"$2"`, etc.

  ## Example

      # Match all records with :admin as third element
      spec = MatchSpec.match_object({:"$1", :"$2", :admin})
      :ets.select(table, spec)
  """
  @spec match_object(tuple | atom) :: match_spec
  def match_object(pattern) do
    [{pattern, [], [:"$_"]}]
  end

  @doc """
  Create a match spec for deletion.

  Returns a match spec that, when used with `:ets.select_delete/2`,
  deletes matching records.

  ## Parameters

    - `pattern`: The match pattern with `:"$1"`, `:"$2"`, etc.

  ## Example

      # Delete all records with :expired status
      spec = MatchSpec.delete_match({:"$1", :"$2", :expired})
      :ets.select_delete(table, spec)
  """
  @spec delete_match(tuple | atom) :: match_spec
  def delete_match(pattern) do
    [{pattern, [], [true]}]
  end

  @doc """
  Compile a match spec for repeated use.

  Compiled match specs are more efficient when the same query is
  executed multiple times.

  Note: In Erlang/Elixir, `:ets.select/2` can accept either:
  - A match spec (list) directly
  - A compiled match spec (reference) from `:ets.match_spec_compile/1`

  ## Example

      spec = MatchSpec.match({:"$1", :_}, [:"$1"])
      compiled = MatchSpec.compile(spec)

      # Later, use with :ets.select(table, compiled)
  """
  @spec compile(match_spec) :: :ets.comp_match_spec() | match_spec
  def compile(spec) when is_list(spec) do
    :ets.match_spec_compile(spec)
  end

  @doc """
  Run a match spec against a table.

  Accepts either a compiled match spec or a regular match spec list.
  """
  @spec select(atom | :ets.tid(), :ets.comp_match_spec() | match_spec) :: [term]
  def select(table, spec) do
    :ets.select(table, spec)
  end

  @doc """
  Run a match spec for deletion.

  Accepts either a compiled match spec or a regular match spec list.
  """
  @spec select_delete(atom | :ets.tid(), :ets.comp_match_spec() | match_spec) :: non_neg_integer
  def select_delete(table, spec) do
    :ets.select_delete(table, spec)
  end

  @doc """
  Create a match spec to get all records.

  **Note:** `:"$_"` is only valid in the *body* of a match spec, not in the
  pattern.  A generic "match all" select spec that works for any tuple arity
  does not exist — the pattern must match the stored tuple structure exactly.

  Prefer `:ets.tab2list/1` when you need all records from a table, or use
  `match_object/1` with an explicit pattern like `{:"$1", :"$2"}`.
  """
  @spec all_records() :: match_spec
  def all_records() do
    [{:"$1", [], [:"$1"]}]
  end

  @doc """
  Create a match spec with a guard condition.

  ## Parameters

    - `pattern`: The match pattern
    - `guards`: List of guard conditions (tuples)
    - `body`: List of expressions to return

  ## Example

      # Match records where the second element is > 100
      spec = MatchSpec.with_guard(
        {:"$1", :"$2"},
        [{:>, :"$2", 100}],
        [:"$_"]
      )
  """
  @spec with_guard(tuple | atom, [tuple], [term]) :: match_spec
  def with_guard(pattern, guards, body) when is_list(guards) and is_list(body) do
    [{pattern, guards, body}]
  end
end
