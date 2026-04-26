defmodule Duxedo.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/soot-iot/duxedo"

  def project do
    [
      app: :duxedo,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp description do
    "Device metrics on DuckDB and Arrow: collects telemetry, stores in-memory and on-disk, queries via Dux."
  end

  defp package do
    [
      licenses: ["MIT"],
      files: ~w(lib .formatter.exs mix.exs README* LICENSE*),
      links: %{
        "GitHub" => @source_url
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end

  defp deps do
    [
      {:dux, "~> 0.3"},
      {:adbc, "~> 0.7"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},
      {:telemetry_metrics, "~> 0.6 or ~> 1.0"},
      {:elixir_uuid, "> 1.2.0"},
      {:term_ui, "~> 1.0.0-rc", optional: true}
    ]
  end
end
