defmodule Falkor.Relation do
  import Falkor.Render

  import Enum

  defstruct [:id, :initial_node, :terminal_node, :rel_type, prop: %{}]

  def new(initial_node, terminal_node, type, prop \\ %{}) do
    new(
      initial_node: initial_node,
      terminal_node: terminal_node,
      rel_type: type,
      prop: prop
    )
  end

  def new(opt) when is_list(opt) do
    new(Map.new(opt))
  end

  def new(map) do
    struct(__MODULE__, map)
  end

  def index_key(graph, rel_type, key) do
    Falkor.query(graph, "CREATE INDEX FOR ()-[r:#{rel_type}]-() ON (r.#{key})")
  end

  def render(relations, nodes) do
    related_nodes_not_in_nodes =
      relations
      |> flat_map(&[&1.initial_node.prop.id, &1.terminal_node.prop.id])
      |> uniq
      |> reject(&Map.has_key?(nodes, &1))
      # TODO isnt there a render function for this already?
      |> map(&"(#{Falkor.Node.get_tag(&1)} {id: '#{&1}'})")

    match_rendered =
      case related_nodes_not_in_nodes do
        [] -> ""
        nodes -> "MATCH\n#{join(nodes, ",\n")}\n"
      end

    {match_rendered, Enum.map(relations, &render_one/1)}
  end

  defp render_one(relation) do
    "(#{relation.initial_node.tag})-#{render_arrow(relation)}->(#{relation.terminal_node.tag})"
  end

  defp render_arrow(relation) do
    rendered_props = render_props(relation.prop)
    rendered_type = render_type(relation.rel_type)

    if rendered_props || rendered_type do
      "[#{rendered_type}#{rendered_props}]"
    end
  end

  defp render_type(rel_type) when rel_type in ["", nil], do: nil
  defp render_type(rel_type), do: ":#{rel_type}"
end
