test_that("get_source_attr()", {
  # can get known_source attrs
  expect_identical(get_source_attr("fanniemae_2016Q4", "dim"), c(22180168L, 31L))

  # and can get test_source attrs
  expect_identical(get_source_attr("nyctaxi_sample", "dim"), c(998L, 18L))
})

test_that("ensure_source error handling", {
  expect_error(
    ensure_source("not_a_source"),
    "not_a_source is not a known source"
  )
})

test_that("source_filename()", {
  expect_identical(
    source_filename("fanniemae_2016Q4"),
    "fanniemae_2016Q4.csv.gz"
  )
})

# create a temporary directory to be used as the data directory
temp_dir <- tempfile()
dir.create(temp_dir)

withr::with_envvar(
  list(ARROWBENCH_LOCAL_DIR = temp_dir),
  test_that("ensure_format", {
    # there are no temp files yet
    expect_false(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.parquet")))
    expect_false(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.csv")))
    expect_false(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.csv.gz")))

    # we can transform from one format to another
    ensure_format("nyctaxi_sample", "parquet")
    expect_true(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.parquet")))

    ensure_format("nyctaxi_sample", "csv", "gzip")
    expect_true(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.csv.gz")))

    # but because we started as a csv, this doesn't create a new file in the
    # temp, instead references it in-situ
    out <- ensure_format("nyctaxi_sample", "csv")
    expect_identical(out, ensure_source("nyctaxi_sample"))
    expect_false(file.exists(file.path(temp_dir, "data", "temp", "nyctaxi_sample.csv")))
  })
)
