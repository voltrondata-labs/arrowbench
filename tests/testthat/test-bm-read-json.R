test_that("read_json setup", {
  defaults <- get_default_args(read_json$setup)
  expect_named(defaults, c("source", "reader", "compression", "output_format", "rbinder"), ignore.order = TRUE)
})

test_that("read_json benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      read_json,
      source = "fanniemae_sample",
      compression = "uncompressed",
      cpu_count = arrow::cpu_count()
    )
  )
})

wipe_results()