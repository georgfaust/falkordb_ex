defmodule FalkorTest do
  use ExUnit.Case

  @redis_endpoint "redis://localhost:6379"

  @movies_create_query """
  CREATE
    (charlie:Person {name: 'Charlie Sheen', age: 55}),
    (martin:Person {name: 'Martin Sheen', age: 72}),
    (michael:Person {name: 'Michael Douglas', age: 75}),
    (oliver:Person {name: 'Oliver Stone', age: 85}),
    (rob:Person {name: 'Rob Reiner', age: 77}),
    (wallStreet:Movie {title: 'Wall Street'}),
    (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),
    (martin)-[:ACTED_IN {role: 'Carl Fox'}]->(wallStreet),
    (michael)-[:ACTED_IN {role: 'Gordon Gekko'}]->(wallStreet),
    (oliver)-[:DIRECTED]->(wallStreet),
    (thePresident:Movie {title: 'The American President'}),
    (martin)-[:ACTED_IN {role: 'A.J. MacInerney'}]->(thePresident),
    (michael)-[:ACTED_IN {role: 'President Andrew Shepherd'}]->(thePresident),
    (rob)-[:DIRECTED]->(thePresident),
    (martin)-[:FATHER_OF]->(charlie)
  """

  setup_all do
    empty_graph = Falkor.Graph.new("empty", @redis_endpoint)
    movies_graph = Falkor.Graph.new("movies", @redis_endpoint)

    # make sure there is no graph leftover from a crash before
    Falkor.delete(empty_graph)
    Falkor.delete(movies_graph)

    on_exit(fn ->
      :ok = Falkor.stop(empty_graph)
      :ok = Falkor.stop(movies_graph)
    end)

    %{movies_graph: movies_graph, empty_graph: empty_graph}
  end

  setup context do
    dbg(context.movies_graph)
    {:ok, _} = Falkor.create(context.movies_graph, @movies_create_query)

    on_exit(fn ->
      Falkor.delete(context.empty_graph)
      Falkor.delete(context.movies_graph)
    end)

    :ok
  end

  test "delete, then delete again and get expected error", context do
    dbg(context.movies_graph)
    assert :ok = Falkor.delete(context.movies_graph)

    assert {:error, %{message: "ERR Invalid graph operation on empty key"}} =
             Falkor.delete(context.movies_graph)
  end

  # TODO
  test "merge", context do
    assert context
    #   # TODO look into meta
    #   # {:ok, merge_result} = Falkor.merge(context.graph, "MERGE (:person { name: 'Michael Douglas' })")
    #   # {:ok, merge_result} = Falkor.merge(context.graph, "MERGE (:person { name: 'Michael Douglas' })")

    #   # # TODO laut stat wird hier node created. Sollte die nicht updated werden?
    #   # {:ok, merge_result} = Falkor.merge(context.graph, "MERGE (:person { name: 'Michael Douglas', age: 100 })")
    #   # assert Map.get(merge_result.statistics, "Nodes created") == "1"
  end

  test "explain", context do
    query = "MATCH ()-->(n) RETURN n"
    assert {:ok, ["Results", "    Project" | _]} = Falkor.explain(context.movies_graph, query)
  end

  @tag :aaa
  test "create from query, read all, delete and recreate from read structs", context do
    assert {:ok, result} = Falkor.create(context.empty_graph, @movies_create_query)

    assert %{
             "Cached execution" => 0,
             "Labels added" => 2,
             "Nodes created" => 7,
             "Properties set" => 17,
             "Relationships created" => 8
           } = result.meta

    nodes =
      context.empty_graph
      |> Falkor.query!("MATCH (n) RETURN n", rows_to_maps: false)
      |> Map.get(:rows)
      |> List.flatten()

    nodes_by_id = Map.new(nodes, &{&1.id, &1})

    relations =
      context.empty_graph
      |> Falkor.query!("MATCH ()-[r]->() RETURN r", rows_to_maps: false)
      |> Map.get(:rows)
      |> List.flatten()
      |> Enum.map(
        &%{
          &1
          | initial_node: nodes_by_id[&1.initial_node],
            terminal_node: nodes_by_id[&1.terminal_node]
        }
      )

    Falkor.delete(context.empty_graph)

    assert {:ok, result} = Falkor.create(context.empty_graph, nodes, relations)

    assert %{
             "Cached execution" => 0,
             "Labels added" => 2,
             "Nodes created" => 7,
             # NOTE: now more props as the prop.id is now set.
             # TODO: I think we need to enforce setting of id-prop with a create query string.
             #  -- or find another way that prop.id
             "Properties set" => 24,
             "Relationships created" => 8
           } = result.meta
  end

  # in py this is different!? https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L131
  # https://docs.falkordb.com/datatypes.html#nulls
  test "null cannot be stored as a property value.", context do
    node = Falkor.Node.new(%{labels: [:label], prop: %{null: nil}})
    assert {:ok, _} = Falkor.create(context.empty_graph, [node], [])

    assert %Falkor.Node{labels: [:label], prop: %{}} =
             Falkor.single!(context.empty_graph, "MATCH (n) RETURN n")
  end

  test "catching null in list", context do
    assert {
             :error,
             %Redix.Error{
               message:
                 "Property values can only be of primitive types or arrays of primitive types"
             }
           } =
             Falkor.create(
               context.empty_graph,
               [Falkor.Node.new(%{labels: [:label], prop: %{illegal_list: [1, nil]}})],
               []
             )
  end

  test "complex properties", context do
    node =
      Falkor.Node.new(%{
        labels: [:label_1, :label_2, :label_3, :label_4],
        prop: %{
          list: [
            true,
            false,
            1,
            2.3,
            "string",
            [true, false, "this", "is", 1.0, "nested", ["L", 1, "ST"]]
          ],
          string: "String",
          interger: 1,
          float: 1.0,
          bool_true: true,
          bool_false: false
        }
      })

    assert {:ok, result} = Falkor.create(context.empty_graph, [node], [])

    assert %{
             "Labels added" => 4,
             "Nodes created" => 1,
             "Properties set" => 7
           } = result.meta

    result = Falkor.single!(context.empty_graph, "MATCH (n) RETURN n")

    assert %Falkor.Node{
             labels: [:label_1, :label_2, :label_3, :label_4],
             prop: %{
               float: 1.0,
               interger: 1,
               list: [
                 true,
                 false,
                 1,
                 2.3,
                 "string",
                 [true, false, "this", "is", 1.0, "nested", ["L", 1, "ST"]]
               ],
               string: "String",
               bool_false: false,
               bool_true: true
             }
           } = result
  end

  # TODO
  test "procedures", context do
    Falkor.Procedure.call!(context.movies_graph, "dbms.procedures", [], ["name"])
  end

  @tag :create
  test "create", context do
    a = Falkor.Node.new(%{labels: ["A"], prop: %{v: "a"}})
    b = Falkor.Node.new(%{labels: ["A"], prop: %{v: "b"}})
    c = Falkor.Node.new(%{labels: ["A"], prop: %{v: "c"}})

    ab = %Falkor.Relation{
      rel_type: "E1",
      initial_node: a,
      terminal_node: b,
      prop: %{v: "b"}
    }

    bc = %Falkor.Relation{
      rel_type: "E1",
      initial_node: b,
      terminal_node: c,
      prop: %{v: "c"}
    }

    assert {:ok, _} = Falkor.create(context.empty_graph, [a, b], [])
    assert {:ok, _} = Falkor.create(context.empty_graph, [c], [ab, bc])

    # TODO do a meaningful assert here
  end

  test "BFS", context do
    # Construct a graph with the form:
    # (a)-[:E1]->(b:B)-[:E1]->(c), (b)-[:E2]->(d)-[:E1]->(e)
    a = Falkor.Node.new(%{labels: ["A"], prop: %{v: "a"}})
    b = Falkor.Node.new(%{labels: ["A"], prop: %{v: "b"}})
    c = Falkor.Node.new(%{labels: ["A"], prop: %{v: "c"}})
    d = Falkor.Node.new(%{labels: ["A"], prop: %{v: "d"}})
    e = Falkor.Node.new(%{labels: ["A"], prop: %{v: "e"}})

    # Edges have the same property as their destination
    ab = %Falkor.Relation{
      rel_type: "E1",
      initial_node: a,
      terminal_node: b,
      prop: %{v: "b"}
    }

    bc = %Falkor.Relation{
      rel_type: "E1",
      initial_node: b,
      terminal_node: c,
      prop: %{v: "c"}
    }

    bd = %Falkor.Relation{
      rel_type: "E2",
      initial_node: b,
      terminal_node: d,
      prop: %{v: "d"}
    }

    de = %Falkor.Relation{
      rel_type: "E1",
      initial_node: d,
      terminal_node: e,
      prop: %{v: "e"}
    }

    assert {:ok, _} = Falkor.create(context.empty_graph, [a, b, c, d, e], [ab, bc, bd, de])

    query = "MATCH (a {v: 'a'}) CALL algo.BFS(a, 0, 'E1') YIELD nodes RETURN [n IN nodes | n.v]"
    assert [[["b", "c"]]] == Falkor.query!(context.empty_graph, query, rows_to_maps: false).rows

    query = """
    MATCH (start_node {v: 'a'})
    CALL algo.BFS(start_node, 0, 'E1')
    YIELD nodes, edges
    RETURN start_node, nodes, edges as relations
    """

    result = Falkor.single_row!(context.empty_graph, query)

    nodes_by_id = Map.new([result.start_node | result.nodes], &{&1.id, &1})

    assert [
             %Falkor.Relation{
               initial_node: %Falkor.Node{labels: [:A], prop: %{v: "a"}},
               terminal_node: %Falkor.Node{labels: [:A], prop: %{v: "b"}},
               rel_type: :E1,
               prop: %{v: "b"}
             },
             %Falkor.Relation{
               initial_node: %Falkor.Node{labels: [:A], prop: %{v: "b"}},
               terminal_node: %Falkor.Node{labels: [:A], prop: %{v: "c"}},
               rel_type: :E1,
               prop: %{v: "c"}
             }
           ] =
             Enum.map(
               result.relations,
               &%{
                 &1
                 | initial_node: nodes_by_id[&1.initial_node],
                   terminal_node: nodes_by_id[&1.terminal_node]
               }
             )
  end

  test "path", context do
    assert 5 ==
             length(
               Falkor.query!(
                 context.movies_graph,
                 "MATCH p=(:Person)-[:ACTED_IN]->(:Movie) RETURN p ORDER BY p",
                 rows_to_maps: false
               ).rows
             )

    # TODO single! does not work with paths
    assert [
             [
               %Falkor.Relation{
                 prop: %{},
                 initial_node: %Falkor.Node{
                   prop: %{age: 72, name: "Martin Sheen"},
                   labels: [:Person]
                 },
                 terminal_node: %Falkor.Node{
                   prop: %{age: 55, name: "Charlie Sheen"},
                   labels: [:Person]
                 },
                 rel_type: :FATHER_OF
               }
             ]
           ] =
             Falkor.single_row!(
               context.movies_graph,
               "MATCH p=(:Person)-[:FATHER_OF]->(:Person) RETURN p",
               rows_to_maps: false
             )
  end

  @latitude 1
  @longitude 2.2
  test "all value types", context do
    query = """
    RETURN [
      1,
      2.3,
      '4',
      true,
      false,
      NULL,
      null,
      [1,2,3],
      {a: 1, b: 2},
      point({latitude: #{@latitude}, longitude: #{@longitude}})
    ]
    """

    result = Falkor.single_row!(context.empty_graph, query, rows_to_maps: false)

    assert [
             [
               1,
               2.3,
               "4",
               true,
               false,
               nil,
               nil,
               [1, 2, 3],
               %{"a" => 1, "b" => 2},
               %{latitude: latitude, longitude: longitude}
             ]
           ] = result

    assert abs(latitude - @latitude) < 0.000001
    assert abs(longitude - @longitude) < 0.000001
  end

  @old_persons_query "MATCH (p:Person) WHERE p.age > 60 RETURN p"
  @expect_in_plan_after_indexing "        Node By Index Scan | (p:Person)"
  test "index node prop key", context do
    {:ok, plan_before_indexing} = Falkor.explain(context.movies_graph, @old_persons_query)
    refute @expect_in_plan_after_indexing in plan_before_indexing

    assert {
             :ok,
             %{
               meta: %{"Indices created" => 1}
             }
           } = Falkor.Node.index_key(context.movies_graph, "Person", "age")

    # this seems to take some time.
    Process.sleep(10)

    # TODO use Falkor.procudure.indexes but I do not understand the result fully yet!
    {:ok, plan_after_indexing} = Falkor.explain(context.movies_graph, @old_persons_query)
    assert @expect_in_plan_after_indexing in plan_after_indexing
  end

  test "index relation prop key", context do
    _ = context
    # TODO
  end

  test "render params" do
    assert "CYPHER a=1\n" == Falkor.Render.render_params(%{a: 1})
    assert "CYPHER a=1.1\n" == Falkor.Render.render_params(%{a: 1.1})
    assert "CYPHER a='foo'\n" == Falkor.Render.render_params(%{a: "foo"})
    assert "CYPHER a=[1,2,3]\n" == Falkor.Render.render_params(%{a: [1, 2, 3]})
    assert "CYPHER a={foo:1,bar:2}\n" == Falkor.Render.render_params(%{a: %{foo: 1, bar: 2}})

    assert "CYPHER a='foo' b=1 c=1.1 d=[1,2,3] e={foo:1,bar:2}\n" ==
             Falkor.Render.render_params(
               a: "foo",
               b: 1,
               c: 1.1,
               d: [1, 2, 3],
               e: %{foo: 1, bar: 2}
             )
  end

  # TODO
  # def test_index_response(client):
  #   result_set = client.graph().query("CREATE INDEX ON :person(age)")
  #   assert 1 == result_set.indices_created

  #   result_set = client.graph().query("CREATE INDEX ON :person(age)")
  #   assert 0 == result_set.indices_created

  #   result_set = client.graph().query("DROP INDEX ON :person(age)")
  #   assert 1 == result_set.indices_deleted

  #   with pytest.raises(ResponseError):
  #       client.graph().query("DROP INDEX ON :person(age)")

  # TODO optional match
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L230

  # TODO cached exec
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L254

  # TODO slowlog
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L271

  # TODO query timeout
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L284

  # TODO RO-query
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L297

  # TODO profile
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L305

  # TODO config
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L320

  # TODO list_keys
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L351

  # TODO cache-sync !! (does the python client keep a complete cache of the graph?)
  # https://github.com/redis/redis-py/blob/cc4bc1a544d1030aec1696baef2861064fa8dd1c/tests/test_graph.py#L398
end
