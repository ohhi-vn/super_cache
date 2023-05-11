# SuperCache

## Introduce

This is a auto scale & distriubted cache library for Elixir.

## Design

 Sequencer flow of api (on a node):

```mermaid
sequenceDiagram
  participant Client
  participant Api
  participant Partition
  participant Storage

  Client->>Api: Add new tuple to cache
  Api->>Partition: Get partition
  Partition->>Api: Your partition
  Api->>Storage: Put new/update tuple
  Storage->>Api: Result
  Api->>Client: Result
  
  Client->>Api: Get data for key/pattern
  Api->>Partition: Get partition for key/pattern
  Partition->>Api: Your patition
  Api->>Storage: Get data for key/pattern
  Storage->>Api: Data for key
  Api->>Client: Your data
```

(If diagram doesn't show, please install mermaid support extension for VS Code)

Simple module flow api:

```mermaid
graph LR
Client(Client) --> Api(Api)
    Api-->|get partition|Part(Partition holder)
    Api-->|Partition1| E1(Partition Storage 1)
    Api-->|Partition2| E2(Partition Storage 2)
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

