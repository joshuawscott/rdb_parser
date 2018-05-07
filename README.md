# RdbParser

Parses a redis backup (.rdb) file.

## Current features:

Parses strings, sets, and lists, with or without expirations.

Other formats will be supported eventaully (Pull requests welcome :))

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

## Usage

Store the contents of a dump file in a map called `database`:

```elixir
database =
  "dump.rdb"
  |> RdbParser.stream_entries()
  |> Enum.reduce(%{}, fn
    {:entry, {key, value, metadata}}, accum ->
      Map.put(accum, key, {value, metadata})

    _, accum ->
      # Ignore the non-data entries
      accum
  end)
```

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
