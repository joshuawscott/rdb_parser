# RdbParser

Parses a redis backup (.rdb) file.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `rdb_parser` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:rdb_parser, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [hexdocs.pm/rdb_parser](https://hexdocs.pm/rdb_parser)

## Testing

The tests use Redix to connect to a running Redis server, insert keys, and save the database.

To work, you must start redis-server with the default options from the root of the repo:
```
redis-server
```

Then you can run the tests with:
```
mix test
```
