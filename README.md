# SuperCache

## Introduce

This is a auto scale & distriubted cache library for Elixir.

## Design

 Sequencer flow of api:

```sequence
Client -> Api: Add new term to cache
Api -> Hash: Get partition for term (based on key)
Hash -> Api: Your partition
Api -> Partition: Put new/update term
Partition -> Api: Result
Api -> Client: Result

Client -> Api: Get data for key/pattern
Api -> Hash: Get partition for key/pattern
Hash -> Api: Your patition
Api -> Partition: Get data for key/pattern
Partition -> Api: Data for key
Api -> Client: Your data
```


```mermaid
graph LR
Client[Hard edge] -->Api(Round edge)
    Api --> Hash{Decision}
    Api -->|Partition1| D[Result one]
    Api -->|Partition2| E[Result two]
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `super_cache` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:super_cache, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/super_cache>.

