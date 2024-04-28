defmodule Falkor do
  import Enum

  def query(graph, query_, opts \\ []) do
    params = Keyword.get(opts, :params)
    dbg(params)
    query = maybe_read_query_from_file(query_)
    query_with_params = Falkor.Render.render_params(params) <> query

    IO.puts(query_with_params)

    case send_cmd(graph, :query, query_with_params) do
      {:ok, result} -> {:ok, Falkor.ParseResult.parse(graph, result, opts)}
      error -> error
    end |> dbg
  end

  defp maybe_read_query_from_file("file:" <> path), do: File.read!(path)
  defp maybe_read_query_from_file(query), do: query

  def query!(graph, query, opts \\ []) do
    case query(graph, query, opts) do
      {:ok, parsed_result} -> parsed_result
      error -> raise(inspect(error))
    end
  end

  def single_row!(graph, query, opts \\ []) do
    case query!(graph, query, opts).rows do
      [row] -> row
      rows -> raise("expected exactly one row, #{length(rows)} returned")
    end
  end

  def single!(graph, query, opts \\ []) do
    opts = Keyword.put(opts, :rows_to_maps, false)

    case single_row!(graph, query, opts) do
      [solution] -> solution
      solutions -> raise("expected exactly one solution, got #{inspect(solutions)}")
    end
  end

  def explain(graph, query) do
    send_cmd(graph, :explain, query)
  end

  def create(graph, query) when is_binary(query) do
    Falkor.query(graph, query)
  end

  def create(graph, [%Falkor.Node{} | _] = nodes, relations) do
    nodes_rendered = map(nodes, &Falkor.Node.render/1)
    nodes_mapping = Map.new(nodes, &{&1.prop.id, &1})
    {match_rendered, relations_rendered} = Falkor.Relation.render(relations, nodes_mapping)

    q = "#{match_rendered}CREATE \n#{join(nodes_rendered ++ relations_rendered, ",\n")}"

    # IO.puts(q)

    query(graph, q)
  end

  def create(_, _, _), do: raise("query string or node/relation list expected")

  def delete(graph) do
    case send_cmd(graph, :delete) do
      {:ok, "OK"} -> :ok
      {:ok, _} = unexpected -> {:error_unexpected, unexpected}
      error -> error
    end
  end

  def send_cmd(graph, cmd), do: send_cmd_(graph.conn, cmd, graph.name, [])
  def send_cmd(graph, cmd, query), do: send_cmd_(graph.conn, cmd, graph.name, [query])

  @commands %{
    query: {"GRAPH.QUERY", ["--compact"]},
    explain: {"GRAPH.EXPLAIN", []},
    delete: {"GRAPH.DELETE", []}
  }
  def send_cmd_(conn, cmd, graph_name, query) do
    {cmd_, opts} = @commands[cmd]
    Redix.command(conn, [cmd_, graph_name] ++ query ++ opts) |> dbg
  end

  def stop(graph) do
    Redix.stop(graph.conn)
  end
end
