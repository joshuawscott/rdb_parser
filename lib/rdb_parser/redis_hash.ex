defmodule RdbParser.RedisHash do
  @moduledoc false

  alias RdbParser.RedisList
  alias RdbParser.RedisString

  @spec parse_ziplist(binary) :: :incomplete | {map(), binary}
  def parse_ziplist(binary) do
    with {data_structure_length, rest} <- RdbParser.parse_length(binary),
         <<ziplist::binary-size(data_structure_length), rest::binary>> <- rest,
         list when is_list(list) <- RedisList.parse_ziplist(ziplist) do
      map =
        list
        |> Enum.chunk_every(2)
        |> Map.new(fn [k, v] -> {k, v} end)

      {map, rest}
    else
      _ -> :incomplete
    end
  end

  @spec parse_hash(binary) :: :incomplete | {map(), binary}
  def parse_hash(binary) do
    with {num_entries, rest} <- RdbParser.parse_length(binary),
         {map, binary} <- parse_hash_entries(rest, [], num_entries) do
      {map, binary}
    else
      :incomplete -> :incomplete
    end
  end

  def parse_hash_entries(binary, entries_left) do
    parse_hash_entries(binary, [], entries_left)
  end

  def parse_hash_entries(binary, parsed_entries, 0) do
    {Map.new(parsed_entries), binary}
  end

  def parse_hash_entries(binary, parsed_entries, entries_left) do
    with {key, rest} <- RedisString.parse(binary),
         {value, rest} <- RedisString.parse(rest) do
      parse_hash_entries(rest, [{key, value} | parsed_entries], entries_left - 1)
    else
      :incomplete -> :incomplete
    end
  end
end
