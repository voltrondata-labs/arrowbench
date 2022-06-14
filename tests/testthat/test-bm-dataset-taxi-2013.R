# Can't run benchmark because the data takes a long time to download (~1h), plus running is slow (~6m)
test_that("dataset_taxi_2013 exists", {
  defaults <- get_default_args(dataset_taxi_2013$setup)

  expect_named(defaults, c("query"))
  expect_equal(
    defaults$query,
    c("basic", "payment_type_crd", "small_no_files", "dims")
  )
})
