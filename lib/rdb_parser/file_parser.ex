defmodule RdbParser.FileParser do
  @moduledoc """
  This does the actual work of parsing a .rdb file. This module can be used directly
  if a filename and function is passed to the `parse_file`
  """

  @type field_type :: :entry | :aux | :version | :resizedb | :selectdb | :eof

  # Some
  @type redis_value :: binary() | MapSet.t() | list() | map()

  @type callback_signature ::
          (:version, integer() -> any())
          | (:resizedb, {:main | :expire, integer()} -> any())
          | (:selectdb, db_number :: integer() -> any())
          | (:aux, {key :: binary(), value :: redis_value} -> any())
          | (:entry, {key :: binary(), value :: redis_value, Keyword.t()} -> any())
          | (:eof, checksum :: binary() -> any())

  @doc """
  Pass a file name and a callback function that will be called with an atom
  representing the type, and a second argument that represents the data from that
  entry. For normal data (keys) this is a 3-tuple. The 3-tuple
  is {key::binary, value::term, metadata::Keyword.t}

  See the `callback_signature` type to understand what values you may need to accept.

  This streams the passed file, so it is possible to parse files of arbitrary size.
  """
  @spec parse_file(String.t(), callback_signature()) :: :ok | {:error, binary()}
  def parse_file(file, callback) do
    create_port()

    unparsed =
      file
      |> File.stream!([], 65536)
      |> Stream.scan("", fn chunk, accum ->
        case parse(accum <> chunk, callback) do
          :ok -> true
          {:incomplete, rest} -> rest
        end
      end)
      |> Stream.run()

    close_port()

    case unparsed do
      :ok -> :ok
      extra -> {:error, extra}
    end
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

  @doc """
  parse looks for opcodes to determine what should be parsed out.
  """
  def parse(<<@rdb_file_header::binary, version::binary-size(4), rest::binary>>, func) do
    func.(:version, String.to_integer(version))
    parse(rest, func)
  end

  def parse(<<@rdb_opcode_aux, rest::binary>>, func) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      func.(:aux, {key, value})
      parse(rest, func)
    else
      :incomplete -> {:incomplete, rest}
    end
  end

  def parse(<<@rdb_opcode_resizedb, rest::binary>>, func) do
    case parse_length(rest) do
      :incomplete ->
        {:incomplete, rest}

      {len, rest} ->
        {expirelen, rest} = parse_length(rest)
        func.(:resizedb, {:main, len, nil})
        func.(:resizedb, {:expire, expirelen})
        parse(rest, func)
    end
  end

  def parse(
        <<@rdb_opcode_expiretime, expiration_time::little-unsigned-integer-size(32),
          @rdb_type_string, rest::binary>>,
        func
      ) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      func.(:expire, {key, value, expires: expiration_time})
      parse(rest, func)
    else
      :incomplete -> {:incomplete, rest}
    end
  end

  def parse(
        <<@rdb_opcode_expiretime_ms, expiration_time::little-unsigned-integer-size(64),
          @rdb_type_string, rest::binary>>,
        func
      ) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      func.(:entry, {key, value, expire_ms: expiration_time})
      parse(rest, func)
    else
      :incomplete -> {:incomplete, rest}
    end
  end

  def parse(<<@rdb_opcode_selectdb, database_id::size(8), rest::binary>>, func) do
    func.(:database_id, database_id)
    parse(rest, func)
  end

  def parse(<<@rdb_opcode_eof, checksum::binary-size(8)>>, func) do
    func.(:eof, checksum)
    :ok
  end

  def parse(<<@rdb_type_string, rest::binary>>, func) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_string(rest) do
      func.(:entry, {key, value, []})
      parse(rest, func)
    else
      :incomplete -> {:incomplete, rest}
    end
  end

  def parse(<<@rdb_type_set, rest::binary>>, func) do
    with {key, rest} <- parse_string(rest),
         {value, rest} <- parse_set(rest) do
      func.(:entry, {key, value, []})
      parse(rest, func)
    else
      :incomplete -> {:incomplete, rest}
    end
  end

  # Fallback case
  def parse(rest, _func) do
    {:incomplete, rest}
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

  def parse_length(rest), do: :incomplete

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
    {compressed_len, rest} = parse_length(data)
    {original_len, rest} = parse_length(rest)

    case rest do
      <<raw_lzf::binary-size(compressed_len), rest::binary>> ->
        {decompress(raw_lzf), rest}

      _ ->
        :incomplete
    end
  end

  def parse_string(binary) do
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

  defp lzf_port do
    :ets.lookup_element(:rdb_state, :lzf, 2)
  end

  defp decompress(binary) do
    port = lzf_port()
    send(port, {self(), {:command, binary}})

    receive do
      {^port, {:data, data}} -> data
      {^port, :closed} -> raise "lzf port closed"
      {:EXIT, ^port, reason} -> raise "lzf crashed: #{inspect(reason)}"
      other -> raise "Received #{inspect(other)}"
    after
      1_000 -> raise "Nothing after 1s"
    end
  end

  defp create_port do
    :rdb_state = :ets.new(:rdb_state, [:named_table])
    lzf_port = Port.open({:spawn, "ruby lzf.rb"}, [:binary, {:packet, 4}])
    true = :ets.insert_new(:rdb_state, {:lzf, lzf_port})
    lzf_port
  end

  defp close_port do
    port = lzf_port()
    send(port, {self(), :close})

    receive do
      {^port, :closed} -> :ok
      err -> raise "Close failed: #{inspect(err)}"
    end
  end
end
