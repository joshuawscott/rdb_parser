defmodule RdbParser.FileParserStreamTest do
  use ExUnit.Case, async: false

  alias RdbParser.FileParserStream

  def save(redis) do
    Redix.command!(redis, ["save"])
  end

  # Returns a callback that inserts entries into the agent's map.
  def collector_callback() do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    callback = fn
      :entry, {key, value, []} ->
        Agent.update(agent, fn entries -> Map.put(entries, key, {value, []}) end)

      :entry, {key, value, metadata} ->
        Agent.update(agent, fn entries -> Map.put(entries, key, {value, metadata}) end)

      _type, _data ->
        :ok
    end

    {agent, callback}
  end

  def get_entries(agent) do
    Agent.get(agent, fn entries -> entries end)
  end

  def get_milliseconds() do
    System.convert_time_unit(System.os_time(), :native, :milliseconds)
  end

  setup do
    {:ok, redis} = Redix.start_link()
    Redix.command(redis, ["flushall"])
    save(redis)

    %{
      redis: redis
    }
  end

  test "parsing a simple string", %{redis: redis} do
    Redix.command(redis, ["SET", "mykey", "myvalue"])
    save(redis)
    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb")
    |> Enum.each(&callback)

    entries = get_entries(agent)
    assert %{"mykey" => {"myvalue", []}} = entries
  end

  @tag :skip
  test "parsing a string with an expire", %{redis: redis} do
    # earlier than expiration
    beginning = get_milliseconds() + 60_000

    Redix.command(redis, ["SET", "mykey", "myvalue", "EX", "60"])
    save(redis)
    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb", callback)

    # later than expiration
    ending = get_milliseconds() + 60_000

    %{"mykey" => {"myvalue", [expire_ms: expiration]}} = get_entries(agent)
    assert beginning <= expiration
    assert ending >= expiration
  end

  @tag :skip
  test "parsing a set", %{redis: redis} do
    Redix.command(redis, ["SADD", "myset", "one"])
    Redix.command(redis, ["SADD", "myset", "two"])
    Redix.command(redis, ["SADD", "myset", "three"])
    save(redis)
    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb", callback)

    %{"myset" => {set, []}} = get_entries(agent)
    assert MapSet.member?(set, "one")
    assert MapSet.member?(set, "two")
    assert MapSet.member?(set, "three")
  end

  @tag :skip
  test "parsing a compressed string", %{redis: redis} do
    # long repeating strings are compressed in the dump
    key = String.duplicate("ab", 100)
    value = String.duplicate("ba", 100)
    Redix.command(redis, ["SET", key, value])
    save(redis)
    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb", callback)
    entries = get_entries(agent)
    assert Map.has_key?(entries, key)
    assert {value, []} == entries[key]
  end

  @tag :skip
  test "parsing a mix of keys", %{redis: redis} do
    # earlier than expiration
    beginning = get_milliseconds() + 60_000

    Redix.command(redis, ["set", "mykey", "myval"])
    Redix.command(redis, ["set", "myexpkey", "myexpval", "ex", "60"])
    Redix.command(redis, ["sadd", "myset", "one"])
    Redix.command(redis, ["sadd", "myset", "two"])
    Redix.command(redis, ["save"])
    save(redis)

    # later than expiration
    ending = get_milliseconds() + 60_000

    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb", callback)
    %{
      "mykey" => {"myval", []},
      "myexpkey" => {"myexpval", [expire_ms: expire_ms]},
      "myset" => {myset, []}
    } = get_entries(agent)

    assert expire_ms >= beginning
    assert expire_ms <= ending
    assert MapSet.member? myset, "one"
    assert MapSet.member? myset, "two"
  end

  @tag :skip
  test "parsing many keys", %{redis: redis} do
    Enum.each(1..10_000, fn n ->
      {:ok, "OK"} = Redix.command(redis, ["SET", "mykey#{n}", "myval#{n}"])
    end)
    {:ok, _} = Redix.command(redis, ["SAVE"])
    {agent, callback} = collector_callback()
    FileParserStream.parse_file("dump.rdb", callback)
    entries = get_entries(agent)
    assert 10_000 == map_size(entries)
    Enum.each(1..10_000, fn n ->
      key = "mykey#{n}"
      expected = "myval#{n}"

      %{^key => {val, []}} = entries
      assert expected == val
    end)
  end
end
