require 'lzf'

STDOUT.sync = true

def receive_input
  encoded_length = STDIN.read(4)
  return nil unless encoded_length
  length = encoded_length.unpack("N").first
  STDIN.read(length)
end

while line = receive_input() do
  begin
    decompressed = LZF.decompress(line)
    STDOUT.write([decompressed.bytesize].pack("N"))
    STDOUT.write(decompressed)
  rescue Exception => e
    STDERR.puts line.bytes.map {|b| sprintf("%02X", b) }.join(" ")
    raise e
  end
end
