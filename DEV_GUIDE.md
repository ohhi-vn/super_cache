# Guide for develop SuperCache

Clone [repo](https://github.com/ohhi-vn/super_cache)

Run:

```bash
mix deps.get
```

For using Tidewave:

```bash
mix tidewave
```

Go to [http://localhost:4000/tidewave](http://localhost:4000/tidewave)


## Using as local dependency

in `mix.exs` file add deps like:

```elixir
defp deps do
  base_dir = "/your/base/path"

  [
    {:super_cache, path: Path.join(base_dir, "super_cache")}
    
    # or using git repo, using override: true if you want to override in sub deps.
    # {:super_cache, git: "https://github.com/your_account/super_cache", override: true}
  ]
end
```
