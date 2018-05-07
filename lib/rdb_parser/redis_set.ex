defmodule RdbParser.RedisSet do
  @moduledoc false
  # Parses redis sets from the rdb format
  # RedisSet.parse looks at the first byte to determine how the length is encoded, then takes the
  # length bytes and extracts the set values.

  alias RdbParser.RedisString

  @spec parse(binary) :: :incomplete | MapSet.t()
  def parse(binary) do
    {num_entries, rest} = RdbParser.parse_length(binary)

    case parse_set_elements(rest, num_entries) do
      :incomplete -> :incomplete
      {set, rest} -> {set, rest}
    end
  end

  def parse_intset(binary) do
    with {byte_length, rest} <- RdbParser.parse_length(binary),
         payload_length <- byte_length - 8,
         <<encoding::little-integer-32, _len::little-integer-32,
           payload::binary-size(payload_length), rest::binary>> <- rest do
      # encoding is the number of bytes per integer
      bits_per_int = 8 * encoding
      set = extract_ints(payload, bits_per_int)
      {set, rest}
    else
      _ -> :incomplete
    end
  end

  # parse_set_elements returns {set, rest} because the byte length is not determinate until we are
  # done iterating the set.
  defp parse_set_elements(rest, entries_left) do
    parse_set_elements(rest, entries_left, [])
  end

  defp parse_set_elements(rest, 0, list) do
    {MapSet.new(list), rest}
  end

  defp parse_set_elements(rest, entries_left, list) do
    case RedisString.parse(rest) do
      :incomplete -> :incomplete
      {str, rest} -> parse_set_elements(rest, entries_left - 1, [str | list])
    end
  end

  defp extract_ints(payload, bits_per_int) do
    extract_ints(payload, bits_per_int, [])
  end

  defp extract_ints("", _bits_per_int, entries) do
    MapSet.new(entries)
  end

  defp extract_ints(payload, bits_per_int, entries) do
    <<int::little-integer-size(bits_per_int), rest::binary>> = payload
    extract_ints(rest, bits_per_int, [int | entries])
  end
end
