defmodule RdbParserTest.Support do
  def save(redis) do
    Redix.command!(redis, ["save"])
  end

  # Parses the dump.rdb file and returns a map of the :entry elements
  # `key => {value, metadata}`
  def parse_rdb(opts \\ []) do
    "dump.rdb"
    |> RdbParser.stream_entries(opts)
    |> get_entries()
  end

  # Returns a map of the `:entry` elements from the stream
  def get_entries(stream) do
    Enum.reduce(stream, %{}, fn
      {:entry, {key, value, metadata}}, acc ->
        Map.put(acc, key, {value, metadata})

      _ignored, acc ->
        acc
    end)
  end

  def get_milliseconds() do
    System.convert_time_unit(System.os_time(), :native, :milliseconds)
  end

  # Pass normal redis options to append them to the command.
  # `add_key(redis, "mykey", "myvalue", ["EX", 123])` will set the expiration to
  # 123 seconds.
  def add_key(redis, key, value) do
    add_key(redis, key, value, [])
  end

  def add_key(redis, key, value, opts) when is_list(value) do
    args = value ++ opts
    command = ["RPUSH", key | args]
    Redix.command(redis, command)
  end

  def add_key(redis, key, %MapSet{} = value, opts) do
    list = MapSet.to_list(value)
    args = list ++ opts
    Redix.command(redis, ["SADD", key | args])
  end

  def add_key(redis, key, %{} = value, opts) do
    hash =
      value
      |> Enum.flat_map(fn {k, v} -> [k, v] end)

    args = hash ++ opts
    Redix.command(redis, ["HSET", key | args])
  end

  def add_key(redis, key, value, opts) do
    command = ["SET", key] ++ [value | opts]

    Redix.command(redis, command)
  end
end

ExUnit.start()
