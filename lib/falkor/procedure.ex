defmodule Falkor.Procedure do
  import Enum

  import Falkor.Render

  # --- TODO test the procs! see https://github.com/RedisGraph/RedisGraph/blob/master/docs/commands/graph.query.md#procedures

  def call(graph, procedure, args \\ [], yields \\ []) do
    Falkor.send_cmd(graph, :query, render_call(procedure, args, yields))
  end

  def call!(graph, procedure, args \\ [], yields \\ []) do
    {:ok, result} = call(graph, procedure, args, yields)

    result
  end

  # TODO why are the more header cols than value cols (if only one index set)
  def get_indexes(graph) do
    {:ok, indexes_result} = Falkor.Procedure.call(graph, "db.indexes")
    Falkor.ParseResult.parse(graph, indexes_result)
  end

  defp render_call(proc_name, args, yields) do
    "CALL #{proc_name}(#{render_args(args)})#{render_yields(yields)}"
  end

  defp render_args(args), do: map_join(args, ", ", &render_value/1)

  defp render_yields([]), do: ""
  defp render_yields(yields), do: " YIELD #{join(yields, ", ")}"
end
