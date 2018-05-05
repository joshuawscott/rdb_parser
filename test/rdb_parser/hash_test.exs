defmodule RdbParser.HashTest do
  use ExUnit.Case, async: false

  import RdbParserTest.Support

  setup do
    {:ok, redis} = Redix.start_link()
    Redix.command(redis, ["flushall"])
    save(redis)

    %{redis: redis}
  end

  test "basic hash", %{redis: redis} do
    map = %{
      "foo" => "bar"
    }

    add_key(redis, "myhash", map)
    save(redis)
    entries = parse_rdb()
    %{"myhash" => {got_hash, []}} = entries

    assert 1 == map_size(got_hash)
    assert map == got_hash
  end

  test "10K entry hash", %{redis: redis} do
    map = Map.new(1..10_000, fn n -> {"key#{n}", "val#{n}"} end)
    add_key(redis, "myhash", map)
    save(redis)
    entries = parse_rdb()
    %{"myhash" => {got_hash, []}} = entries

    assert 10_000 == map_size(got_hash)
    assert map == got_hash
  end

  test "incomplete in hash", %{redis: redis} do
    map = %{
      "foo" => "bar"
    }

    add_key(redis, "myhash", map)
    save(redis)
    entries = parse_rdb(chunk_size: 1)
    %{"myhash" => {got_hash, []}} = entries

    assert 1 == map_size(got_hash)
    assert map == got_hash
  end
end
