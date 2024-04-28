defmodule Falkor.ParseResult do
  import Enum

  def parse(graph, data, opts \\ [])
  def parse(_graph, [meta] = data, _opts), do: %{meta: parse_meta(meta), raw: data}

  def parse(graph, data, opts) do
    atoms_keys? = Keyword.get(opts, :atom_keys, true)
    rows_to_maps? = Keyword.get(opts, :rows_to_maps, true)

    mappings = get_mappings(graph, atoms_keys?)
    parsed = parse_(data, mappings, atoms_keys?)

    # {t_mappings, mappings} = :timer.tc(__MODULE__, :get_mappings, [graph, atoms_keys?])
    # dbg(t_mappings)

    # {t_parse, parsed} = :timer.tc(__MODULE__, :parse_, [data, mappings, atoms_keys?])
    # dbg(t_parse)
    rows = if rows_to_maps?, do: result_rows_to_maps(parsed), else: parsed.rows

    %{parsed | rows: rows}
  end

  def parse_([header, rows, meta] = data, mappings, atoms_keys?) do
    %{
      header: parse_header(header, atoms_keys?),
      rows: parse_rows(rows, mappings),
      meta: parse_meta(meta),
      mappings: mappings,
      raw: data
    }
  end

  defp parse_rows(rows, mappings) do
    for row <- rows do
      for cell <- row do
        parse_value(cell, mappings)
      end
    end
  end

  def get_mappings(graph, atoms_keys?) do
    Map.new(
      ["db.labels", "db.propertyKeys", "db.relationshipTypes"],
      &get_mapping(graph, &1, atoms_keys?)
    )
  end

  defp get_mapping(graph, proc, atoms_keys?) do
    get_value = fn [value] ->
      if atoms_keys?, do: String.to_atom(value), else: value
    end

    %{header: [header], rows: rows} =
      graph
      |> Falkor.Procedure.call!(proc)
      |> parse_(%{}, false)

    mapping =
      rows
      |> with_index()
      |> Map.new(fn {elem, index} -> {index, get_value.(elem)} end)

    {header, mapping}
  end

  # https://docs.falkordb.com/design/client_spec.html#reading-the-header-row
  defp parse_header(header, atom_keys?) do
    map(header, fn [_, h] -> if atom_keys?, do: String.to_atom(h), else: h end)
  end

  defp parse_meta(meta) do
    meta
    |> map(&String.split(&1, ": "))
    |> Map.new(fn [k, v] ->
      value =
        case Integer.parse(v) do
          {int, ""} -> int
          _ -> v
        end

      {k, value}
    end)
  end

  defp result_rows_to_maps(result) do
    %{rows: rows, header: header} = result

    for row <- rows do
      zip(header, row) |> Map.new()
    end
  end

  # https://docs.falkordb.com/design/client_spec.html
  # https://github.com/FalkorDB/FalkorDB/blob/ce6f659dc76bc846be80149f3bde6affc39c3e45/src/resultset/formatters/resultset_formatter.h#L21-L28
  @value_unknown 0
  @value_null 1
  @value_string 2
  @value_integer 3
  @value_boolean 4
  @value_double 5
  @value_array 6
  @value_relation 7
  @value_node 8
  @value_path 9
  @value_map 10
  @value_point 11

  defp parse_value(value, mappings \\ nil)
  # TODO log this? how to test this?
  defp parse_value([@value_unknown, _value], _mappings), do: nil
  defp parse_value([@value_null, _value], _mappings), do: nil
  defp parse_value([@value_string, value], _mappings), do: value
  defp parse_value([@value_integer, value], _mappings), do: value
  defp parse_value([@value_boolean, value], _mappings), do: String.to_existing_atom(value)
  defp parse_value([@value_double, value], _mappings), do: parse_float(value)
  defp parse_value([@value_array, value], mappings), do: map(value, &parse_value(&1, mappings))

  defp parse_value([@value_relation, value], mappings) do
    [relation_id, relation_index, initial_node, terminal_node, properties] = value

    Falkor.Relation.new(%{
      id: relation_id,
      rel_type: mappings["relationshipType"][relation_index],
      initial_node: initial_node,
      terminal_node: terminal_node,
      prop: parse_properties(properties, mappings["propertyKey"])
    })
  end

  defp parse_value([@value_node, value], mappings) do
    [node_id, label_indexes, properties] = value

    Falkor.Node.new(%{
      id: node_id,
      labels: map(label_indexes, &mappings["label"][&1]),
      prop: parse_properties(properties, mappings["propertyKey"])
    })
  end

  defp parse_value([@value_path, [nodes, relations]], mappings) do
    nodes_by_id = parse_value(nodes, mappings) |> Map.new(&{&1.id, &1})
    relations = parse_value(relations, mappings)

    map(
      relations,
      &%Falkor.Relation{
        &1
        | initial_node: nodes_by_id[&1.initial_node],
          terminal_node: nodes_by_id[&1.terminal_node]
      }
    )
  end

  # TODO atom-keys here?
  defp parse_value([@value_map, value], mappings) do
    value |> chunk_every(2) |> Map.new(fn [k, v] -> {k, parse_value(v, mappings)} end)
  end

  defp parse_value([@value_point, [lat, long]], _mappings) do
    %{latitude: parse_float(lat), longitude: parse_float(long)}
  end

  def parse_properties(properties, mapping) do
    properties |> Map.new(&parse_kv_pair(&1, mapping))
  end

  def parse_kv_pair([key_index, value_type, value], mapping) do
    {
      mapping[key_index],
      parse_value([value_type, value])
    }
  end

  defp parse_float(value) do
    {f, ""} = Float.parse(value)
    f
  end
end
