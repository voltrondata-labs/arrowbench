test_that("custom DuckDB can be installed to and used from a custom lib", {
  # ...and can execute SQL
  expect_equal(
    query_custom_duckdb("SELECT 'thing' as col_name"),
    data.frame(col_name = 'thing')
  )

  # ...and write parquet files
  temp_parquet <- tempfile()
  expect_identical(
    export_custom_duckdb("SELECT 'thing' as col_name", temp_parquet),
    temp_parquet
  )

  expect_equal(
    as.data.frame(arrow::read_parquet(temp_parquet)),
    data.frame(col_name = 'thing', stringsAsFactors = FALSE)
  )
})
