defmodule RdbParserTest do
  use ExUnit.Case, async: false

  import RdbParserTest.Support

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

    entries = parse_rdb()

    assert %{"mykey" => {"myvalue", []}} = entries
  end

  test "parsing a string with an expire", %{redis: redis} do
    # earlier than expiration
    beginning = get_milliseconds() + 60_000

    Redix.command(redis, ["SET", "mykey", "myvalue", "EX", "60"])
    save(redis)

    entries = parse_rdb()

    # later than expiration
    ending = get_milliseconds() + 60_000

    %{"mykey" => {"myvalue", [expire_ms: expiration]}} = entries
    assert beginning <= expiration
    assert ending >= expiration
  end

  test "parsing a compressed string", %{redis: redis} do
    # long repeating strings are compressed in the dump
    key = String.duplicate("ab", 100)
    value = String.duplicate("ba", 100)
    Redix.command(redis, ["SET", key, value])
    save(redis)

    entries = parse_rdb()

    assert Map.has_key?(entries, key)
    assert {value, []} == entries[key]
  end

  for integer <- [-65537, -65536, -65535, -257, -256, -255, -14, -13, -12, -1, 0, 1, 12, 13, 255, 256, 65535, 65536] do
    test "parsing integer #{integer}", %{redis: redis} do
      int = unquote(integer)
      Redix.command(redis, ["SET", "mykey", int])
      save(redis)
      entries = parse_rdb()

      %{"mykey" => {got_int, []}} = entries
      assert int == got_int
    end
  end


  test "parsing a list", %{redis: redis} do
    original_list = ["AAAAAAAAAAA", "AAAAAAAAAA", "A"]
    add_key(redis, "mylist", original_list)
    save(redis)

    entries = parse_rdb()

    %{"mylist" => {parsed_list, []}} = entries
    assert original_list == parsed_list
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

  test "parsing a mix of keys", %{redis: redis} do
    # earlier than expiration
    beginning = get_milliseconds() + 60_000

    Redix.command(redis, ["set", "mykey", "myval"])
    Redix.command(redis, ["set", "myexpkey", "myexpval", "ex", "60"])
    Redix.command(redis, ["sadd", "myset", "one"])
    Redix.command(redis, ["sadd", "myset", "two"])

    save(redis)

    # later than expiration
    ending = get_milliseconds() + 60_000

    entries = parse_rdb()

    %{
      "mykey" => {"myval", []},
      "myexpkey" => {"myexpval", [expire_ms: expire_ms]},
      "myset" => {myset, []}
    } = entries

    assert expire_ms >= beginning
    assert expire_ms <= ending
    assert MapSet.member?(myset, "one")
    assert MapSet.member?(myset, "two")
  end

  test "parsing many keys", %{redis: redis} do
    Enum.each(1..10_000, fn n ->
      {:ok, "OK"} = Redix.command(redis, ["SET", "mykey#{n}", "myval#{n}"])
    end)

    {:ok, _} = Redix.command(redis, ["SAVE"])

    entries = parse_rdb()

    assert 10_000 == map_size(entries)

    Enum.each(1..10_000, fn n ->
      key = "mykey#{n}"
      expected = "myval#{n}"

      %{^key => {val, []}} = entries
      assert expected == val
    end)
  end
end
