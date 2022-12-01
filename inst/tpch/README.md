# Why is there a separate duckdb data directory here?

The DuckDB data generator actually produces data that is _slightly_ out of spec. Specifically, the `_address` columns generate slightly different data. Generally speaking, this isn't a big deal since the queries don't use pattern matches on those columns, but they do show up in some of the answers.

But it's plain to see if you look at the official answers and the duckdb ones for a query that includes a `_address` column:

https://github.com/databricks/tpch-dbgen/blob/6985da461c641fd0d255b214f2d693f1bf08bc33/answers/q2.out
https://github.com/duckdb/duckdb/blob/c0a4ab96c626426961c207f49c19aa81448e91da/extension/tpch/dbgen/answers/sf1/q02.csv