defmodule RdbParser.MixProject do
  use Mix.Project

  def project do
    [
      app: :rdb_parser,
      version: "0.3.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env() == :prod,
      description: """
      Parses an Redis dump backup file (.rdb file) and extracts entries into a Stream for out of
      band processing of data stored in redis.
      """,
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:lzf, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:lzf, "~> 0.1"},
      {:redix, ">= 0.0.0", only: [:test, :dev]},
      {:dialyxir, "~> 0.5", only: [:dev]},
      {:credo, "~> 0.9", only: [:dev]},
      {:ex_doc, ">= 0.0.0", only: [:dev]}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      source_url: "https://github.com/joshuawscott/rdb_parser",
      maintainers: ["Joshua Scott"],
      links: %{"GitHub" => "https://github.com/joshuawscott/rdb_parser"}
    ]
  end

end
