# create a temporary directory to be used as the data directory
temp_dir <- tempfile()
dir.create(temp_dir)

withr::with_envvar(
  list(ARROWBENCH_DATA_DIR = temp_dir), {
  test_that("ensure_format", {
    # there are no temp files yet
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.parquet")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.uncompressed.parquet")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.snappy.parquet")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv.gz")))

    # we can transform from one format to another
    ensure_format("nyctaxi_sample", "parquet")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.uncompressed.parquet")))

    ensure_format("nyctaxi_sample", "parquet", compression = "snappy")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.snappy.parquet")))

    ensure_format("nyctaxi_sample", "parquet", compression = "snappy", chunk_size = 100000)
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.chunk_size_1e+05.snappy.parquet")))

    ensure_format("nyctaxi_sample", "feather", compression = "lz4")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.lz4.feather")))

    # note: this is sliiightly bigger than the chunk_size above, but we get the same rounded value
    ensure_format("nyctaxi_sample", "feather", chunk_size = 100010)
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.chunk_size_1e+05.uncompressed.feather")))

    # But, if the difference is bigger, we get that value reflected
    ensure_format("nyctaxi_sample", "feather", chunk_size = 100100)
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.chunk_size_1.001e+05.uncompressed.feather")))

    ensure_format("nyctaxi_sample", "csv", compression = "gzip")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv.gz")))

    # but because we started as a csv, this doesn't create a new file in the
    # temp, instead references it in-situ
    out <- ensure_format("nyctaxi_sample", "csv")
    expect_identical(out, ensure_source("nyctaxi_sample"))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv")))
  })

  test_that("ensure_format with tpch", {
    # don't test if we are not already trying to install the custom duckdb
    skip_if(Sys.getenv("ARROWBENCH_TEST_CUSTOM_DUCKDB", "") == "")

    # there are no temp files yet
    expect_false(file.exists(file.path(temp_dir, "lineitem_0.001.parquet")))
    expect_false(file.exists(file.path(temp_dir, "temp", "lineitem_0.001.uncompressed.parquet")))

    # we can generate
    tpch_files <- ensure_tpch(0.0001)
    expect_true(file.exists(file.path(temp_dir, "lineitem_0.0001.parquet")))

    # and we can ensure format
    lineitem <- ensure_format(tpch_files[["lineitem"]], "parquet")
    expect_equal(lineitem, file.path(temp_dir, "temp", "lineitem_0.0001.uncompressed.parquet"))
    expect_true(file.exists(file.path(temp_dir, "temp", "lineitem_0.0001.uncompressed.parquet")))
  })
})

test_that("format + compression validation with a df", {
  df <- expand.grid(
    source = "a source",
    lib_path = "some/path",
    format = c("csv", "parquet", "fst"),
    compression = c("gzip", "zstd", "snappy"),
    stringsAsFactors = FALSE
  )

  expect_identical(
    validate_format(df$format, df$compression),
    c(TRUE, TRUE, FALSE, FALSE, TRUE, TRUE, FALSE, TRUE, FALSE)
  )
})

test_that("format + compression validation", {
  expect_true(stop_if_not_valid_format("csv", "gzip"))

  expect_error(
    stop_if_not_valid_format("csv", "snappy"),
    "The format csv does not support snappy compression"
  )
})
