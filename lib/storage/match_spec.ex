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
  """

  @type match_spec :: [{term(), [term()], [term()]}]

  @doc """
  Create a match spec for `:ets.match/2` style queries.

  Returns a match spec that, when used with `:ets.select/2`, returns
  a list of binding lists (like `:ets.match/2`).
  """
  @spec match(tuple | atom, [term]) :: match_spec
  def match(pattern, bindings) when is_list(bindings) do
    # Wrap bindings in a list so :ets.select returns [[binding1, binding2]] like :ets.match
    [{pattern, [], [bindings]}]
  end

  @doc """
  Create a match spec for `:ets.match_object/2` style queries.
  """
  @spec match_object(tuple | atom) :: match_spec
  def match_object(pattern) do
    [{pattern, [], [:"$_"]}]
  end

  @doc """
  Create a match spec for deletion.
  """
  @spec delete_match(tuple | atom) :: match_spec
  def delete_match(pattern) do
    [{pattern, [], [true]}]
  end

  @deprecated "Broken on OTP 28+ (:ets.select/2 rejects compiled refs); pass the plain spec to select/2 instead"
  @spec compile(match_spec) :: :ets.comp_match_spec() | match_spec
  def compile(spec) when is_list(spec) do
    :ets.match_spec_compile(spec)
  end

  @doc """
  Run a match spec against a table.
  """
  @spec select(atom | :ets.tid(), :ets.comp_match_spec() | match_spec) :: [term]
  def select(table, spec) do
    :ets.select(table, spec)
  end

  @doc """
  Run a match spec for deletion.
  """
  @spec select_delete(atom | :ets.tid(), :ets.comp_match_spec() | match_spec) :: non_neg_integer
  def select_delete(table, spec) do
    :ets.select_delete(table, spec)
  end

  @doc """
  Create a match spec to get all records.
  """
  @spec all_records() :: match_spec
  def all_records do
    [{:"$1", [], [:"$1"]}]
  end

  @doc """
  Create a match spec with a guard condition.
  """
  @spec with_guard(tuple | atom, [tuple], [term]) :: match_spec
  def with_guard(pattern, guards, body) when is_list(guards) and is_list(body) do
    [{pattern, guards, body}]
  end

  @doc """
  Recursively extract capture variables (`:"$1"`, `:"$2"`, ...) from a match
  pattern, deduplicate them, and sort them numerically by variable index.

  Supports capture variables at any nesting depth inside tuples and lists, so
  patterns like `{{:kv, :"$1"}, :"$2"}` or `{:rec, :"$2", :"$1"}` work
  correctly. The returned list is sorted by variable number so the output
  binding list is always `[b1, b2, ...]` in numerical order.
  """
  @spec extract_bindings(term()) :: [atom()]
  def extract_bindings(pattern) do
    pattern
    |> collect_captures([])
    |> Enum.uniq()
    |> Enum.sort_by(&capture_index/1)
  end

  defp collect_captures(atom, acc) when is_atom(atom) do
    if capture_atom?(atom) do
      [atom | acc]
    else
      acc
    end
  end

  defp collect_captures(tuple, acc) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reduce(acc, fn elem, acc -> collect_captures(elem, acc) end)
  end

  defp collect_captures(list, acc) when is_list(list) do
    Enum.reduce(list, acc, fn elem, acc -> collect_captures(elem, acc) end)
  end

  defp collect_captures(_other, acc), do: acc

  defp capture_atom?(atom) when is_atom(atom) do
    case Atom.to_string(atom) do
      "$" <> rest ->
        case Integer.parse(rest) do
          {n, ""} when n >= 1 -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  defp capture_index(atom) when is_atom(atom) do
    case Atom.to_string(atom) do
      "$" <> rest ->
        case Integer.parse(rest) do
          {n, ""} -> n
          _ -> 0
        end

      _ ->
        0
    end
  end
end
