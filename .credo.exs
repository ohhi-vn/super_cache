%{
  configs: [
    %{
      name: "default",
      plugins: [{ExSlop, []}, {ExDNA.Credo, []}]
    }
  ]
}
