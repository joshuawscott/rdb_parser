defmodule RdbParser.SetTest do
  use ExUnit.Case, async: false

  import RdbParserTest.Support

  setup do
    {:ok, redis} = Redix.start_link()
    Redix.command(redis, ["flushall"])
    save(redis)

    %{redis: redis}
  end

  test "parsing a set", %{redis: redis} do
    Redix.command(redis, ["SADD", "myset", "one"])
    Redix.command(redis, ["SADD", "myset", "two"])
    Redix.command(redis, ["SADD", "myset", "three"])
    save(redis)

    entries = parse_rdb()

    %{"myset" => {set, []}} = entries
    assert MapSet.member?(set, "one")
    assert MapSet.member?(set, "two")
    assert MapSet.member?(set, "three")
  end

  test "parsing a set with an expiration", %{redis: redis} do
    # earlier than expiration
    beginning = get_milliseconds() + 60_000
    Redix.command(redis, ["SADD", "myset", "one"])
    Redix.command(redis, ["SADD", "myset", "two"])
    Redix.command(redis, ["SADD", "myset", "three"])
    Redix.command(redis, ["EXPIRE", "myset", 60])
    save(redis)

    entries = parse_rdb()

    %{"myset" => {set, [expire_ms: expiration]}} = entries
    assert MapSet.member?(set, "one")
    assert MapSet.member?(set, "two")
    assert MapSet.member?(set, "three")
    # later than expiration
    ending = get_milliseconds() + 60_000
    assert beginning <= expiration
    assert ending >= expiration
  end

  test "parsing a set of integers", %{redis: redis} do
    Redix.command(redis, ["SADD", "myset", "3"])
    Redix.command(redis, ["SADD", "myset", "2"])
    Redix.command(redis, ["SADD", "myset", "1"])
    save(redis)

    entries = parse_rdb(chunk_size: 1)

    %{"myset" => {set, []}} = entries
    assert MapSet.member?(set, 1)
    assert MapSet.member?(set, 2)
    assert MapSet.member?(set, 3)
  end
end
