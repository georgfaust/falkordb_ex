defmodule Falkor.Graph do
  defstruct [:name, :conn]

  def new(name, redis_endpoint) do
    {:ok, conn} = Redix.start_link(redis_endpoint)

    struct(__MODULE__, %{name: name, conn: conn})
  end
end
