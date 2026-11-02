# Used by "mix format"
#
# Filter macOS AppleDouble (._*) metadata files: they are binary sidecars that
# appear on external/network volumes and would otherwise break `mix format`.
files =
  [".formatter.exs", "mix.exs"] ++
    (Path.wildcard("{config,lib,test}/**/*.{ex,exs}")
     |> Enum.reject(&String.contains?(&1, "/._")))

[
  inputs: files
]
