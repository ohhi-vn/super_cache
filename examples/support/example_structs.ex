defmodule ExampleStructs.User do
  @moduledoc """
  Example User struct for SuperCache demonstrations.
  """
  defstruct [:id, :name, :email]
end

defmodule ExampleStructs.Order do
  @moduledoc """
  Example Order struct for SuperCache demonstrations.
  """
  defstruct [:id, :customer, :status, :total]
end

defmodule ExampleStructs.Product do
  @moduledoc """
  Example Product struct for SuperCache demonstrations.
  """
  defstruct [:sku, :name, :price, :category]
end
