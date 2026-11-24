defmodule SuperCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :super_cache,
      version: "1.7.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      config_path: "config/config.exs",
      elixirc_paths: elixirc_paths(Mix.env()),

      # Docs
      name: "SuperCache",
      source_url: "https://github.com/ohhi-vn/super_cache",
      homepage_url: "https://ohhi.vn",
      docs: docs(),
      description: description(),
      package: package(),
      aliases: aliases(),
      dialyzer: dialyzer(),
      test_coverage: [
        ignore_modules: [
          # Dev-only mix task and test-support module — not library code.
          Mix.Tasks.BenchmarkEts,
          SuperCache.ClusterCase,
          TelemetryStub
        ]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    dev_app =
      if Mix.env() == :dev do
        [:observer, :wx, :tools, :runtime_tools]
      else
        []
      end

    [
      mod: {SuperCache.Application, []},
      extra_applications: [:logger] ++ dev_app
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:benchee, "~> 1.5", only: :dev},
      {:excoveralls, "~> 0.18", only: :test},

      # Opt-in Raft consensus replication (`:raft` replication mode).
      {:raft_ex, "~> 0.3.1"},

      # Code quality
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5", only: [:dev, :test], runtime: false}
    ]
  end

  defp description() do
    "An in-memory caching library using tuples as the core data type, with support for structs, key/value pairs, queues, and stacks. Includes experimental distributed caching."
  end

  defp package() do
    [
      # Ship the AI-agent skills and their AGENTS.md entry point so consuming
      # applications can copy them straight out of their deps checkout.
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "LICENSE",
        ".formatter.exs",
        "skills"
      ],
      maintainers: ["Manh Van Vu"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ohhi-vn/super_cache",
        "About us" => "https://ohhi.vn/"
      }
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
          "F A Q" -> "FAQ"
          no_change -> no_change
        end

      {String.to_atom(path),
       [
         title: title,
         default: title == "Guide"
       ]}
    end)
  end

  defp dialyzer() do
    [
      # Logger and Mix.Task are runtime applications but not part of the
      # default core PLT, so without these Dialyzer reports their functions as
      # unknown.
      plt_add_apps: [:logger, :mix]
    ]
  end

  defp aliases() do
    [
      # Normal unit tests — no distribution required.
      test: ["test --exclude cluster"],

      # Cluster integration tests — spawns peer nodes via :peer.
      # VM flags (--name, --cookie) must be passed via --erl to the runtime,
      # not directly to mix test which does not understand them.
      "test.cluster": [
        "cmd elixir --name primary@127.0.0.1 --cookie test_secret -S mix test --only cluster"
      ],

      # Testing & Coverage
      coveralls: ["test --cover", "coveralls.html"],

      # Code Quality
      quality: ["format --check-formatted", "credo --strict", "dialyzer"],

      # Run every gate that must pass before committing. The test stage spawns
      # a fresh Mix process in the test environment; cluster tests stay opt-in
      # via `mix test.cluster`.
      precommit: [
        "quality",
        "cmd env MIX_ENV=test mix test --exclude cluster"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "examples/support"]
  defp elixirc_paths(_), do: ["lib"]
end
