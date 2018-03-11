defmodule RdbParser.ListTest do
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

  test "list with a 255 length element", %{redis: redis} do
    long_elem = String.duplicate("A", 255)
    startlist = [long_elem, "A", "A"]
    midlist = ["A", long_elem, "A"]
    endlist = ["A", "A", long_elem]
    add_key(redis, "mylist_start", startlist)
    add_key(redis, "mylist_mid", midlist)
    add_key(redis, "mylist_end", endlist)

    save(redis)

    entries = parse_rdb()

    %{"mylist_start" => {got_startlist, []}} = entries
    %{"mylist_mid" => {got_midlist, []}} = entries
    %{"mylist_end" => {got_endlist, []}} = entries

    assert startlist == got_startlist
    assert midlist == got_midlist
    assert endlist == got_endlist
  end

  # This exceeds the 8K size limit of a single ziplist, so we will handle the case
  # of quicklists containing multiple ziplists.
  test "list with 1,000,000 elements", %{redis: redis} do
    list =
      1..1_000_000
      |> Enum.map(&"elem#{&1}")

    add_key(redis, "mylist", list)

    save(redis)

    # With the default 64K chunk size, this tends to time out.
    entries = parse_rdb(chunk_size: 8_000_000)

    %{"mylist" => {got_list, []}} = entries

    assert 1_000_000 == length(got_list)
    assert list == got_list
  end

  test "list with integers", %{redis: redis} do
    list = 1..10 |> Enum.into([])

    add_key(redis, "mylist", list)

    save(redis)

    entries = parse_rdb()

    %{"mylist" => {got_list, []}} = entries

    assert 10 == length(got_list)
    assert list == got_list
  end

  test "list integer boundries", %{redis: redis} do
    # ziplists encode integers as 8, 16, 24, 32, 64 bit signed integers.
    # Redis encodes strings of numbers as a number if it's a 64 bit integer.
    list = [
      # 64-bit
      9_223_372_036_854_775_806,
      9_223_372_036_854_775_807,
      "9_223_372_036_854_775_808",
      -9_223_372_036_854_775_807,
      -9_223_372_036_854_775_808,
      "-9_223_372_036_854_775_809",
      # 32-bit
      -2_147_483_647,
      -2_147_483_648,
      -2_147_483_649,
      2_147_483_646,
      2_147_483_647,
      2_147_483_648,
      # 24-bit
      8_388_606,
      8_388_607,
      8_388_608,
      -8_388_607,
      -8_388_608,
      -8_388_609,
      # 16-bit
      -32_767,
      -32_768,
      -32_769,
      32_766,
      32_767,
      32_768,
      # 8-bit
      126,
      127,
      128,
      -127,
      -128,
      -129
    ]

    add_key(redis, "mylist", list)

    save(redis)

    entries = parse_rdb()

    %{"mylist" => {got_list, []}} = entries

    assert list == got_list
  end
end
