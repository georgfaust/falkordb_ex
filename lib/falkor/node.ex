defmodule Falkor.Node do
  import Enum

  import Falkor.Render

  defstruct [:id, :tag, :labels, prop: %{}]

  def new(map) do
    id = Map.get(map.prop, :id, Uniq.UUID.uuid1(:hex))
    prop = Map.put(map.prop, :id, id)
    map = Map.merge(map, %{tag: get_tag(id), prop: prop})
    struct(__MODULE__, map)
  end

  def new(labels, prop) do
    new(%{labels: labels, prop: prop})
  end

  def get_tag(id), do: "TAG_#{id}"

  def index_key(graph, label, key) do
    Falkor.query(graph, "CREATE INDEX FOR (n:#{label}) ON (n.#{key})")
  end

  def render(%__MODULE__{tag: nil}), do: raise("tag is required for create. HINT: use new()")

  def render(node) do
    "(#{node.tag}:#{render_labels(node.labels)}#{render_props(node.prop)})"
  end

  defp render_labels(labels), do: join(labels, ":")
end
