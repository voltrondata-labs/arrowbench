
test_that("custom DuckDB can be installed to and used from a custom lib", {
  # an unexpected error shouldn't trigger an install
  expect_error(
    ensure_custom_duckdb(lib = new.env()),
    "An unexpected error occured"
  )

  # an empty library without duckdb should trigger an install
  # but one can request not to install
  tf <- tempfile()
  dir.create(tf)
  on.exit(unlink(tf, recursive = TRUE))

  expect_error(
    ensure_custom_duckdb(tf, install = FALSE),
    "and `install = FALSE`"
  )

  # don't actually do the installing during routine testing
  # because it takes too long
  skip_if(Sys.getenv("ARROWBENCH_TEST_CUSTOM_DUCKDB", "") == "")

  # install a non-custom duckdb and make sure it fails properly
  install.packages("duckdb", lib = tf, quiet = TRUE)
  expect_error(
    ensure_custom_duckdb(tf, install = FALSE),
    "and `install = FALSE`"
  )

  # ...but also make sure it installs
  expect_silent(ensure_custom_duckdb(tf, install = TRUE, quiet = TRUE))

  # ...and works a second time without installing
  expect_silent(ensure_custom_duckdb(tf, install = FALSE))

  # ...and can execute SQL
  expect_equal(
    query_custom_duckdb("SELECT 'thing' as col_name", lib = tf),
    data.frame(col_name = 'thing')
  )

  # ...and write parquet files
  temp_parquet <- tempfile()
  expect_identical(
    export_custom_duckdb("SELECT 'thing' as col_name", temp_parquet, lib = tf),
    temp_parquet
  )

  expect_equal(
    as.data.frame(arrow::read_parquet(temp_parquet)),
    data.frame(col_name = 'thing', stringsAsFactors = FALSE)
  )
})
