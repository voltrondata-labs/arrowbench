# create a temporary directory to be used as the data directory
temp_dir <- tempfile()
dir.create(temp_dir)

withr::with_envvar(
  list(ARROWBENCH_DATA_DIR = temp_dir),
  test_that("ensure_format", {
    # there are no temp files yet
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.parquet")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.parquet.snappy")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv")))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv.gz")))

    # we can transform from one format to another
    ensure_format("nyctaxi_sample", "parquet")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.parquet")))

    ensure_format("nyctaxi_sample", "parquet", "snappy")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.parquet.snappy")))

    ensure_format("nyctaxi_sample", "csv", "gzip")
    expect_true(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv.gz")))

    # but because we started as a csv, this doesn't create a new file in the
    # temp, instead references it in-situ
    out <- ensure_format("nyctaxi_sample", "csv")
    expect_identical(out, ensure_source("nyctaxi_sample"))
    expect_false(file.exists(file.path(temp_dir, "temp", "nyctaxi_sample.csv")))
  })
)

test_that("format + compression validatoin", {
  expect_true(validate_format("csv", "gzip"))

  expect_error(
    validate_format("csv", "snappy"),
    "The format csv does not support snappy compression"
    )
})
