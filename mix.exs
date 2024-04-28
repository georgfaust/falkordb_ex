defmodule Falkor.MixProject do
  use Mix.Project

  def project do
    [
      version: "0.0.1",
      app: :falkor,
      elixir: "~> 1.14",
      # start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      # extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Uniq.UUID.uuid1(:hex)
      {:uniq, "~> 0.6"},
      {:redix, "~> 1.1"}
      # {:castore, ">= 0.0.0"},
    ]
  end
end
