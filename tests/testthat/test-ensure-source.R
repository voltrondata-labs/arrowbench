test_that("get_source_attr()", {
  # can get known_source attrs
  expect_identical(get_source_attr("fanniemae_2016Q4", "dim"), c(22180168L, 31L))

  # and can get test_source attrs
  expect_identical(get_source_attr("nyctaxi_sample", "dim"), c(998L, 18L))
})

test_that("get_dataset_attr()", {
  # can get known_source attrs
  expect_identical(get_dataset_attr("taxi_parquet", "dim"), c(1547741381L, 20L))
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
