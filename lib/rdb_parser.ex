defmodule RdbParser do
  @moduledoc """
  Emits a stream that can be used to work through the entries without having to read the entire
  file into memory (which could be impossible).

  Example - this creates a Map from the entries in the rdb file.
  ```
  RdbParser.stream_entries("myredis.rdb")
  |> Enum.reduce(%{}, fn
    {:entry, {key, value, metadata}}, acc ->
      Map.set(acc, key, value)
    _ ->
      acc
  end)
  ```
  """

  alias RdbParser.RedisList
  alias RdbParser.RedisSet
  alias RdbParser.RedisString
  require Logger

  @type field_type :: :entry | :aux | :version | :resizedb | :selectdb | :eof

  @type redis_value :: binary() | MapSet.t() | list() | map()
  @type rdb_entry ::
          {:version, version_number :: integer()}
          | {:resizedb, {:main | :expire, dbsize :: integer()}}
          | {:selectdb, db_number :: integer()}
          | {:aux, {key :: binary(), value :: redis_value}}
          | {:entry, {key :: binary(), value :: redis_value, Keyword.t()}}
          | {:eof, checksum :: binary()}
  @type stream_option :: :chunk_size
  @type stream_options :: [stream_option]

  @doc """
  Pass a filename and `opts`.
  The filename is read in chunks and parsed to avoid reading the entire backup
  file into memory.

  Options:
  * `:chunk_size`: The size of the chunks to read from the file at a time. This
    can be tuned based on expected sizes of the keys. Typically if you have
    larger keys, you should increase this. Default: 65,536 bytes.

  The returned stream emits `rdb_entry` entries. Each is a tuple, with the first
  element reflecting the entry type.
  * `{:version, version_number :: integer()}`: The version of the rdb file.
  * `{:resizedb, {:main, dbsize :: integer()}}`: The number of keys in the database
  * `{:resizedb, {:expire, dbsize :: integer()}`: The number of keys with expirations
  * `{:selectdb, db_number :: integer()}`: The database number that will be read.
  * `{:aux, {key :: binary(), value :: redis_value}}`: A piece of metadata.
  * `{:entry, {key :: binary(), value :: redis_value, metadata :: Keyword.t }}`: A key/value pair.
    The metadata contains expiration information if any.
  * `{:eof, checksum :: binary()}`: If the file is parsed fully, this will be the last entry.

  `stream_entries` returns a stream, so the result can be passed to
  `Task.async_stream` or `Flow` functions. Note that using an Enum function will
  start the enumeration, so an `Enum.map` will build the entire list of entries
  before doing additional steps. For parsing larger datasets it's recommended
  to only use Stream or Flow type constructs and only to use Enum.reduce at the
  end of the function chain to avoid running out of memory.
  """
  @spec stream_entries(binary, stream_options) :: Enumerable.t()
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
  @rdb_type_list 1
  @rdb_type_set 2
  # @rdb_type_zset 3
  # @rdb_type_hash 4
  # @rdb_type_zset_2 5 # zset version 2 with doubles stored in binary
  # @rdb_type_module 6

  # Encoded Types
  # @rdb_type_hash_zipmap 9
  @rdb_type_list_ziplist 10
  # @rdb_type_set_intset 11
  # @rdb_type_zset_ziplist 12
  # @rdb_type_hash_ziplist 13
  @rdb_type_list_quicklist 14
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

  # Version entry
  def parse(<<@rdb_file_header, version::binary-size(4), rest::binary>>, entries) do
    entry = {:version, String.to_integer(version)}
    parse(rest, [entry | entries])
  end

  # Metadata "Aux" fields
  def parse(<<@rdb_opcode_aux, rest::binary>> = orig, entries) do
    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- RedisString.parse(rest) do
      entry = {:aux, {key, value}}
      parse(rest, [entry | entries])
    else
      :incomplete -> {:lists.reverse(entries), orig}
    end
  end

  # This is a directive that redis uses to know how many hashtable slots it
  # needs to allocate. It gives the number of entries.
  def parse(<<@rdb_opcode_resizedb, rest::binary>> = orig, entries) do
    with {mainlen, rest} <- parse_length(rest),
         {expirelen, rest} <- parse_length(rest) do
      entry = {:resizedb, {mainlen, expirelen}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in resizedb")
        {:lists.reverse(entries), orig}
    end
  end

  # This tells which database is about to be read
  def parse(<<@rdb_opcode_selectdb, database_id::size(8), rest::binary>>, entries) do
    parse(rest, [{:database_id, database_id} | entries])
  end

  # This is a k/v pair with an expiration in seconds
  def parse(
        <<@rdb_opcode_expiretime, expiration_time::little-unsigned-integer-size(32),
          datatype::size(8), rest::binary>> = orig,
        entries
      ) do
    parse_fun = parse_fun_for(datatype)

    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- parse_fun.(rest) do
      parse(rest, [{:expire, {key, value, expires: expiration_time}} | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in expire")
        {:lists.reverse(entries), orig}
    end
  end

  # This is an k/v pair with an expiration in milliseconds
  def parse(
        <<@rdb_opcode_expiretime_ms, expiration_time::little-unsigned-integer-size(64),
          datatype::size(8), rest::binary>> = orig,
        entries
      ) do
    parse_fun = parse_fun_for(datatype)

    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- parse_fun.(rest) do
      entry = {:entry, {key, value, expire_ms: expiration_time}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in expire_ms")
        {:lists.reverse(entries), orig}
    end
  end

  # This is a STRING type key/value pair
  def parse(<<@rdb_type_string, rest::binary>> = orig, entries) do
    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- RedisString.parse(rest) do
      entry = {:entry, {key, value, []}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in string")
        {:lists.reverse(entries), orig}
    end
  end

  # This is a LIST type key/value pair
  def parse(<<@rdb_type_list, rest::binary>> = orig, entries) do
    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- RedisList.parse(rest) do
      entry = {:entry, {key, value, []}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in list")
        {:lists.reverse(entries), orig}
    end
  end

  # This is a LIST type key/value pair QUICKLIST encoded
  def parse(<<@rdb_type_list_quicklist, rest::binary>> = orig, entries) do
    with {key, rest2} <- RedisString.parse(rest),
         {value, rest3} <- RedisList.parse_quicklist(rest2) do
      entry = {:entry, {key, value, []}}
      parse(rest3, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in list")
        {:lists.reverse(entries), orig}
    end
  end

  # This is a SET type key/value pair
  def parse(<<@rdb_type_set, rest::binary>> = orig, entries) do
    with {key, rest} <- RedisString.parse(rest),
         {value, rest} <- RedisSet.parse(rest) do
      entry = {:entry, {key, value, []}}
      parse(rest, [entry | entries])
    else
      :incomplete ->
        Logger.debug("incomplete in set")
        {:lists.reverse(entries), orig}
    end
  end

  def parse(<<unsupported_type::size(8), rest::binary>> = orig, entries)
      when unsupported_type <= 15 do
    Logger.warn("unsupported key type #{unsupported_type}")
  end

  # Fallback case - this should mean that we don't have the right length in
  # the current chunk.
  def parse(orig, entries) do
    {:lists.reverse(entries), orig}
  end

  @doc """
  parse_length returns {length, rest} where length is the decoded length, and
  rest is the remaining part of the binary.
  """
  # 6-bit length
  def parse_length(<<0::size(2), len::size(6), rest::binary>>), do: {len, rest}

  # 14-bit length
  def parse_length(<<1::size(2), len::size(14), rest::binary>>), do: {len, rest}

  # 32-bit lenth
  def parse_length(
        <<2::size(2), 0::size(6), len::little-unsigned-integer-size(32), rest::binary>>
      ) do
    {len, rest}
  end

  def parse_length(
        <<3::size(2), 0::size(6), len::little-unsigned-integer-size(64), rest::binary>>
      ) do
    {len, rest}
  end

  def parse_length(_rest), do: :incomplete

  defp parse_fun_for(@rdb_type_string), do: &RedisString.parse/1
  defp parse_fun_for(@rdb_type_list), do: &RedisList.parse/1
  defp parse_fun_for(@rdb_type_set), do: &RedisSet.parse/1
end
