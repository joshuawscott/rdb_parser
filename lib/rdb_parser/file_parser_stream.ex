defmodule RdbParser.FileParserStream do
  @moduledoc """
  Emits a stream that can be used to work through the entries without having to read the entire
  file into memory (which could be impossible).

  This does the actual work of parsing a .rdb file. This module can be used directly
  if a filename and function is passed to the `parse_file`
  """

  require Logger

  @type field_type :: :entry | :aux | :version | :resizedb | :selectdb | :eof

  # Some
  @type redis_value :: binary() | MapSet.t() | list() | map()
  @type rdb_entry ::
          {:version, integer()}
          | {:resizedb, {:main | :expire, integer()}}
          | {:selectdb, db_number :: integer()}
          | {:aux, {key :: binary(), value :: redis_value}}
          | {:entry, {key :: binary(), value :: redis_value, Keyword.t()}}
          | {:eof, checksum :: binary()}

  def stream_entries(filename, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, 65536)
    filename
    |> File.stream!([], chunk_size)
    |> Stream.scan({[], ""}, fn chunk, {_entries, leftover} ->
      parse(leftover <> chunk)
    end)
    |> Stream.flat_map(fn {entries, _leftover} -> entries end)
  end

  ##
  # Taken from rdb.h
  # https://github.com/antirez/redis/blob/unstable/src/rdb.h
  ##

  # Opcodes
  # 0xFA
  @rdb_opcode_aux 250
  # 0xFB
  @rdb_opcode_resizedb 251
  # 0xFC
  @rdb_opcode_expiretime_ms 252
  # 0xFD
  @rdb_opcode_expiretime 253
  # 0xFE
  @rdb_opcode_selectdb 254
  # 0xFF
  @rdb_opcode_eof 255

  # Basic Types
  @rdb_type_string 0
  # @rdb_type_list 1
  @rdb_type_set 2
  # @rdb_type_zset 3
  # @rdb_type_hash 4
  # @rdb_type_zset_2 5 # zset version 2 with doubles stored in binary
  # @rdb_type_module 6

  # Encoded Types
  # @rdb_type_hash_zipmap 9
  # @rdb_type_list_ziplist 10
  # @rdb_type_set_intset 11
  # @rdb_type_zset_ziplist 12
  # @rdb_type_hash_ziplist 13
  # @rdb_type_list_quicklist 14
  # @rdb_type_stream_listpacks 15

  @rdb_file_header "REDIS"

  @doc false
  @spec parse(binary) :: {[rdb_entry], binary()}
  def parse(bin), do: parse(bin, [])

  @doc false
  @spec parse(binary, [rdb_entry]) :: {[rdb_entry], binary()}
  # Parsed to end of chunk
  def parse("", entries) do
    {:lists.reverse(entries), ""}
  end

  # Parsed to end of file
  def parse(<<@rdb_opcode_eof, checksum::binary-size(8)>>, entries) do
    entry = {:eof, checksum}
    {:lists.reverse([entry | entries]), ""}
  end

  def parse(<<@rdb_file_header, version::binary-size(4), rest::binary>>, entries) do
    entry = {:version, String.to_integer(version)}
    parse(rest, [entry | entries])
  end

  def parse(<<@rdb_opcode_aux, rest::binary>> = orig, entries) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      entry = {:aux, {key, value}}
      parse(rest, [entry | entries])
    else
      :incomplete -> {:lists.reverse(entries), orig}
    end
  end

  def parse(<<@rdb_opcode_resizedb, rest::binary>> = orig, entries) do
    case parse_length(rest) do
      :incomplete ->
        Logger.debug("incomplete in resizedb")
        {:lists.reverse(entries), orig}

      {len, rest} ->
        {expirelen, rest} = parse_length(rest)
        entry = {:resizedb, {len, expirelen}}
        parse(rest, [entry | entries])
    end
  end

  def parse(
        <<@rdb_opcode_expiretime, expiration_time::little-unsigned-integer-size(32),
          @rdb_type_string, rest::binary>> = orig,
        entries
      ) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      parse(rest, [{:expire, {key, value, expires: expiration_time}} | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in expire")
        {:lists.reverse(entries), orig}
    end
  end

  def parse(
        <<@rdb_opcode_expiretime_ms, expiration_time::little-unsigned-integer-size(64),
          @rdb_type_string, rest::binary>> = orig,
        entries
      ) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      entry = {:entry, {key, value, expire_ms: expiration_time}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in expire_ms")
        {:lists.reverse(entries), orig}
    end
  end

  def parse(<<@rdb_opcode_selectdb, database_id::size(8), rest::binary>>, entries) do
    parse(rest, [{:database_id, database_id} | entries])
  end

  def parse(<<@rdb_type_string, rest::binary>> = orig, entries) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      entry = {:entry, {key, value, []}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in string")
        {:lists.reverse(entries), orig}
    end
  end

  def parse(<<@rdb_type_set, rest::binary>> = orig, entries) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_set(rest) do
      entry = {:entry, {key, value, []}}
      parse(rest, [entry|entries])
    else
      :incomplete ->
        Logger.debug("incomplete in set")
        {:lists.reverse(entries), orig}
    end
  end

  # Fallback case - this should mean that we don't have the right length in
  # the current chunk.
  def parse(orig, entries) do
    {:lists.reverse(entries), orig}
  end

  def parse_length(<<0::size(2), len::size(6), rest::binary>>), do: {len, rest}
  def parse_length(<<1::size(2), len::size(14), rest::binary>>), do: {len, rest}

  def parse_length(
        <<2::size(2), 0::size(6), len::little-unsigned-integer-size(32), rest::binary>>
      ),
      do: {len, rest}

  def parse_length(
        <<3::size(2), 0::size(6), len::little-unsigned-integer-size(64), rest::binary>>
      ),
      do: {len, rest}

  def parse_length(_rest), do: :incomplete

  @doc """
  parse_string looks at the first byte to determine how the length is encoded, then takes the next
  length bytes as the string value and returns {string, rest}.
  """
  # short string (6-bit length)
  def parse_string(<<0::size(2), len::size(6), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # med string (14-bit length)
  def parse_string(<<1::size(2), len::size(14), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # long string (32-bit length)
  def parse_string(<<128::size(8), len::size(32), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # long long string (64-bit length)
  def parse_string(<<129::size(8), len::size(64), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # 8-bit signed integer
  def parse_string(<<3::size(2), 0::size(6), num::integer-signed-8, rest::binary>>) do
    {num, rest}
  end

  # 16-bit signed integer
  def parse_string(<<3::size(2), 1::size(6), num::integer-signed-16, rest::binary>>) do
    {num, rest}
  end

  # 32-bit signed integer
  def parse_string(<<3::size(2), 2::size(6), num::integer-signed-32, rest::binary>>) do
    {num, rest}
  end

  # LZF compressed chunk
  def parse_string(<<195::size(8), data::binary>>) do
    with {compressed_len, rest} <- parse_length(data),
         {original_len, rest} <- parse_length(rest),
         <<raw_lzf::binary-size(compressed_len), rest::binary>> <- rest do
      formatted_lzf =
        <<"ZV", 1, compressed_len::size(16), original_len::size(16), raw_lzf::binary>>

      {Lzf.decompress(formatted_lzf), rest}
    else
      _ -> :incomplete
    end
  end

  def parse_string(_binary) do
    :incomplete
  end

  def parse_set(binary) do
    {num_entries, rest} = parse_length(binary)

    case parse_set_elements(rest, num_entries, MapSet.new()) do
      :incomplete -> :incomplete
      {set, rest} -> {set, rest}
    end
  end

  def parse_set_elements(rest, 0, set), do: {set, rest}

  def parse_set_elements(rest, entries_left, set) do
    case parse_string(rest) do
      :incomplete -> :incomplete
      {str, rest} -> parse_set_elements(rest, entries_left - 1, MapSet.put(set, str))
    end
  end
end
