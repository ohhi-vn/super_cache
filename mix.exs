defmodule SuperCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :super_cache,
      version: "0.4.1",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      config_path: "config/config.exs",

      # Docs
      name: "SuperCache",
      source_url: "https://github.com/ohhi-vn/super_cache",
      homepage_url: "https://ohhi.vn",
      docs: docs(),
      description: description(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {SuperCache.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:ex_doc, "~> 0.2", only: :dev, runtime: false},
      {:benchee, "~> 1.1", only: :dev},
    ]
  end

  defp description() do
    "A library for cache data in memory. The library uses partition storage for a can cache service a mount of request. We are still developing please don't use for product."
  end

  defp package() do
    [
      maintainers: ["Manh Van Vu", "Tam Nhat Ly"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/ohhi-vn/super_cache", "About us" => "https://ohhi.vn/team"}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: extras()
    ]
  end

  defp extras do
    list =
      "guides/**/*.md"
      |> Path.wildcard()

    list = list ++ ["README.md"]

    list
    |> Enum.map(fn path ->
      title =
        path
        |> Path.basename(".md")
        |> String.split(~r|[-_]|)
        |> Enum.map_join(" ", &String.capitalize/1)
        |> case do
          "F A Q" ->"FAQ"
          no_change -> no_change
        end

      {String.to_atom(path),
        [
          title: title,
          default: title == "Guide"
        ]
      }
    end)
  end
end
