test_that("read_json setup", {
  defaults <- get_default_args(read_json$setup)
  expect_named(defaults, c("source", "reader", "compression", "output_format"), ignore.order = TRUE)
})
