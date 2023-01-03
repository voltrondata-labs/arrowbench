test_that("ensure_source error handling", {
  expect_error(
    ensure_source("not_a_source"),
    "There was an error with datalogistik"
  )
})
