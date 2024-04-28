# Falkor

**experimental FalkorDB client.**

FalkorDB is a fork of the discontinued redisgraph. This should also work with redisgraph.

See https://www.falkordb.com/


## howto ...?

run this

```
docker run -p 6379:6379 -p 3000:3000 -it --rm falkordb/falkordb:latest
```

and then this: `test/falkor_test.exs` - the test should give an idea how to use the client.

