defmodule Falkor.Render do
  import Enum

  # TODO - seems like there must not be NULL in a list.
  #   %Redix.Error{message: "Property values can only be of primitive types or arrays of primitive types"}}
  def render_value(v) when is_map(v), do: render_props(v)
  def render_value(v) when is_list(v), do: "[#{map_join(v, ",", &render_value/1)}]"
  def render_value(v) when is_binary(v), do: "'#{v}'"
  def render_value(nil), do: "NULL"
  def render_value(v), do: inspect(v)

  def render_props(props) when props == %{}, do: nil
  def render_props(props), do: "{#{map_join(props, ",", &render_kv_pair/1)}}"

  defp render_kv_pair({k, v}), do: "#{k}:#{render_value(v)}"

  # TODO test params queries
  def render_params(nil), do: ""

  def render_params(params) do
    params_kv_pairs = map_join(params, " ", fn {k, v} -> "#{Atom.to_string(k)}=#{render_value(v)}" end)
    "CYPHER #{params_kv_pairs}\n"
  end
end
