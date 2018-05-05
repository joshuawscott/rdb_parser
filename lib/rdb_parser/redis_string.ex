defmodule RdbParser.RedisString do
  @moduledoc false
  # Parses redis strings from the rdb format
  # RedisString.parse looks at the first byte to determine how the length is encoded, then takes the
  # next length bytes as the string value and returns {string, rest}.

  @enc_len_32 128
  @enc_len_64 129
  @enc_signed_8 192
  @enc_signed_16 193
  @enc_signed_32 194
  @enc_lzf 195
  # short string (6-bit length)
  @spec parse(binary) :: :incomplete | {integer, binary}
  def parse(<<0::size(2), len::size(6), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # med string (14-bit length)
  def parse(<<1::size(2), len::size(14), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # long string (32-bit length)
  def parse(<<@enc_len_32, len::size(32), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # long long string (64-bit length)
  def parse(<<@enc_len_64, len::size(64), str::binary-size(len), rest::binary>>) do
    {str, rest}
  end

  # 8-bit signed integer
  def parse(<<@enc_signed_8, num::integer-signed-8, rest::binary>>) do
    {num, rest}
  end

  # 16-bit signed integer
  def parse(<<@enc_signed_16, num::little-integer-signed-16, rest::binary>>) do
    {num, rest}
  end

  # 32-bit signed integer
  def parse(<<@enc_signed_32, num::little-integer-signed-32, rest::binary>>) do
    {num, rest}
  end

  # LZF compressed chunk
  def parse(<<@enc_lzf, data::binary>>) do
    with {compressed_len, rest} <- RdbParser.parse_length(data),
         {original_len, rest} <- RdbParser.parse_length(rest),
         <<raw_lzf::binary-size(compressed_len), rest::binary>> <- rest do
      formatted_lzf =
        <<"ZV", 1, compressed_len::size(16), original_len::size(16), raw_lzf::binary>>

      {Lzf.decompress(formatted_lzf), rest}
    else
      _ -> :incomplete
    end
  end

  def parse(_binary) do
    :incomplete
  end
end
