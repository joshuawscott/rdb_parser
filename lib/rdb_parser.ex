defmodule RdbParser do
  @moduledoc """
  RdbParser defines a behaviour `RdbParser. that can be used within your module.

  ### Usage

  Define an `entry/3` callback at minimum in order to use the module. `entry/3` is
  called with a string key and a value that depends on the type of entry present.
  The third field is metadata for the key. This may be an empty list. Typically
  this includes the expiration timestamp if present.

  ### Example
  To have a Module that just prints the length of all the keys:
  ```
  defmodule RedisPrinter do
    @behaviour RdbParser
    def entry(key, value) do
      key
      |> String.length()
      |> IO.inspect()
    end
  end
  ```

  ### Expiration

  Expirations are sent as ether epoch milliseconds or epoch seconds, depending
  on the version of Redis. These are send in either the `:expire` or `:expire_ms`
  key of the metadata keyword list.
  """

  @doc """
  called with the key, value, and any other metadata that would be needed.
  """
  @callback entry(key::binary(), value::term(), metadata::Keyword.t) :: term()

  # Future work: default implementations to handle the non-entry values:
  # * aux
  # * resize_db
  # * selectdb
  # * eof


end
