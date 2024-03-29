library(dplyr, warn.conflicts = FALSE, quietly = TRUE)

test_that("can find the tables used", {
  test_query <- function(input_func) {
    input_func("fun_table") %>%
      collect()
  }
  expect_identical(tables_refed(test_query), "fun_table")

  # can also grab the ones that are from joins
  test_query <- function(input_func) {
    input_func("fun_table") %>%
      inner_join(input_func("less_fun_table"), by = "some_col")
      collect()
  }
  expect_identical(tables_refed(test_query), c("fun_table", "less_fun_table"))

  # a real query
  test_query <- function(input_func) {
    input_func("lineitem") %>%
      select(l_shipdate, l_returnflag, l_linestatus, l_quantity,
             l_extendedprice, l_discount, l_tax) %>%
      filter(l_shipdate <= as.Date("1998-09-02")) %>%
      select(l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax) %>%
      group_by(l_returnflag, l_linestatus) %>%
      summarise(
        sum_qty = sum(l_quantity),
        sum_base_price = sum(l_extendedprice),
        sum_disc_price = sum(l_extendedprice * (1 - l_discount)),
        sum_charge = sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
        avg_qty = mean(l_quantity),
        avg_price = mean(l_extendedprice),
        avg_disc = mean(l_discount),
        count_order = n()
      ) %>%
      arrange(l_returnflag, l_linestatus) %>%
      collect()
  }
  expect_identical(tables_refed(test_query), "lineitem")
})

test_that("tpch_answer (arrowbench)", {
  q01_ans <- tpch_answer(0.01, 1, source = "arrowbench")
  expect_s3_class(q01_ans, "tbl_df")

  q22_ans <- tpch_answer(1, 22, source = "arrowbench")
  expect_s3_class(q22_ans, "tbl_df")
})

# don't test if we are not already trying to install the custom duckdb
skip_if(Sys.getenv("ARROWBENCH_TEST_CUSTOM_DUCKDB", "") == "")

test_that("tpch_answer (duckdb)", {
  q22_ans <- tpch_answer(1, 22, source = "duckdb")
  expect_s3_class(q22_ans, "data.frame")
})

test_that("get_query_func()", {
  out <- get_query_func(1)
  expect_type(out, "closure")
  expect_identical(out, tpc_h_queries[[1]])

  out <- get_query_func(1, engine = "duckdb_sql")
  expect_type(out, "closure")
})

# create a temporary directory to be used as the data directory
temp_dir <- tempfile()
dir.create(temp_dir)

withr::with_envvar(
  list(ARROWBENCH_DATA_DIR = temp_dir), {
    test_that("get_input_func()", {
      input_func <- get_input_func(
        engine = "arrow",
        scale_factor = 0.01,
        query_id = 1,
        format = "parquet",
        compression = "uncompressed"
      )

      linteitem_ds <- input_func("lineitem")
      expect_true(grepl("lineitem_0.01.uncompressed.parquet", linteitem_ds$files, fixed = TRUE))

      input_func <- get_input_func(
        engine = "arrow",
        scale_factor = 0.01,
        query_id = 1,
        format = "parquet",
        compression = "snappy"
      )

      linteitem_ds <- input_func("lineitem")
      expect_true(grepl("lineitem_0.01.snappy.parquet", linteitem_ds$files, fixed = TRUE))
    })
  })

test_that("tpch sql queries", {
  query_01 <- get_sql_tpch_query(1)
  expect_equal(
    query_01,
    "SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;\n"
  )

  # create a new connection to ensure we're using the one that is being based
  con_one <- DBI::dbConnect(duckdb::duckdb())

  # use it to populate the tables needed for query 1
  get_input_func(
    engine = "duckdb",
    scale_factor = 0.001,
    format = "parquet",
    query_id = 1,
    con = con_one
  )

  query_01_func <- get_sql_query_func(1)
  expect_s3_class(query_01_func(con = con_one), "data.frame")

  # clean up
  DBI::dbDisconnect(con_one, shutdown = TRUE)
})


test_that("grepl queries work with dbplyr style sql", {
  # TODO: query 13 seems to not still for some reason
  skip_if_not_installed("duckdb", minimum_version = "0.3.3")
  expect_benchmark_run(
    run_benchmark(
      tpc_h,
      engine = "duckdb",
      query_id = c(2, 9, 14, 16, 20),
      scale_factor = 0.001
    )
  )
})


test_that("query 21 use of logicals passed", {
  expect_benchmark_run(
    run_benchmark(
      tpc_h,
      engine = "duckdb",
      query_id = 21,
      scale_factor = 0.001
    )
  )
})
