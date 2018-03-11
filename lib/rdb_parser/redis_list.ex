defmodule RdbParser.RedisList do
  @moduledoc false

  alias RdbParser.RedisString

  # Encodings for ziplist members. This is a single byte that determines how the
  # length is encoded in the following bytes.
  @enc_8 254
  @enc_16 192
  @enc_24 240
  @enc_32 208
  @enc_64 224
  # @eof 255

  @doc """
  Returns {list, rest} where list is the List of entries, and rest is the
  remaining binary to be parsed.
  """
  @spec parse_quicklist(binary) :: :incomplete | {list(integer | binary), binary}
  def parse_quicklist(data) do
    with {num_ziplists, rest} <- RdbParser.parse_length(data),
         {backward_encoded_ziplists, unused} <- extract_encoded_ziplists(rest, num_ziplists),
         encoded_ziplists <- Enum.reverse(backward_encoded_ziplists),
         list when is_list(list) <- encoded_ziplists |> Enum.map(&parse_ziplist/1) |> List.flatten()
    do
      {list, unused}
    else
      :incomplete -> :incomplete
    end
  end

  def parse_ziplist(
        <<_total_size::little-integer-size(32), _offset_to_tail::little-integer-size(32),
          num_entries::little-integer-size(16), payload::binary>>
      ) do
    parse_ziplist_entries(payload, num_entries, [])
  end

  defp parse_ziplist_entries(_, 0, list) do
    Enum.reverse(list)
  end

  # Previous entry is 32-bit length
  defp parse_ziplist_entries(
         <<254, _prev_len::size(32), rest::binary()>>,
         num_entries,
         list
       ) do
    case parse_ziplist_entry(rest) do
      :incomplete ->
        :incomplete

      {item, rest} ->
        parse_ziplist_entries(rest, num_entries - 1, [item | list])
    end
  end

  # previous entry is 8-bit length
  defp parse_ziplist_entries(
         <<_prev_len::size(8), rest::binary()>>,
         num_entries,
         list
       ) do
    case parse_ziplist_entry(rest) do
      :incomplete ->
        :incomplete

      {item, rest} ->
        parse_ziplist_entries(rest, num_entries - 1, [item | list])
    end
  end

  # 8 bit signed integer
  defp parse_ziplist_entry(<<@enc_8, num::little-signed-integer-size(8), rest::binary>>) do
    {num, rest}
  end

  # 16 bit signed integer
  defp parse_ziplist_entry(<<@enc_16, num::little-signed-size(16), rest::binary>>) do
    {num, rest}
  end

  # 24 bit signed integer
  defp parse_ziplist_entry(<<@enc_24, num::little-signed-size(24), rest::binary>>) do
    {num, rest}
  end

  defp parse_ziplist_entry(<<@enc_32, num::little-signed-size(32), rest::binary>>) do
    {num, rest}
  end

  defp parse_ziplist_entry(<<@enc_64, num::little-signed-size(64), rest::binary>>) do
    {num, rest}
  end

  defp parse_ziplist_entry(<<15::size(4), numcode::size(4), rest::binary>>) do
    {numcode - 1, rest}
  end

  # The 6/14/32 bit length strings are handled the same as a normal string.
  defp parse_ziplist_entry(binary) do
    case RedisString.parse(binary) do
      :incomplete ->
        :incomplete

      {entry, rest} ->
        {entry, rest}
    end
  end

  defp extract_encoded_ziplists(rest, num_ziplists) do
    1..num_ziplists
    |> Enum.reduce({[], rest}, fn _, {encoded_ziplists, rest} ->
      case RedisString.parse(rest) do
        :incomplete ->
          :incomplete

        {encoded_ziplist, unused} ->
          {[encoded_ziplist | encoded_ziplists], unused}
      end
    end)
  end

  # NOTE: THE FOLLOWING IS UNTESTED.
  def parse(binary) do
    {num_entries, rest} = RdbParser.parse_length(binary)
    parse_list_elements(rest, num_entries, [])
  end

  defp parse_list_elements(rest, 0, list), do: {list, rest}

  defp parse_list_elements(rest, entries_left, list) do
    case RedisString.parse(rest) do
      :incomplete -> :incomplete
      {str, rest} -> parse_list_elements(rest, entries_left - 1, [str | list])
    end
  end
end
