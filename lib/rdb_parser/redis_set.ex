defmodule RdbParser.RedisSet do
  @moduledoc false
  # Parses redis sets from the rdb format
  # RedisSet.parse looks at the first byte to determine how the length is encoded, then takes the
  # length bytes and extracts the set values.

  alias RdbParser.RedisString

  @spec parse(binary) :: :incomplete | MapSet.t()
  def parse(binary) do
    {num_entries, rest} = RdbParser.parse_length(binary)

    case parse_set_elements(rest, num_entries, MapSet.new()) do
      :incomplete -> :incomplete
      {set, rest} -> {set, rest}
    end
  end

  defp parse_set_elements(rest, 0, set), do: {set, rest}

  defp parse_set_elements(rest, entries_left, set) do
    case RedisString.parse(rest) do
      :incomplete -> :incomplete
      {str, rest} -> parse_set_elements(rest, entries_left - 1, MapSet.put(set, str))
    end
  end
end
