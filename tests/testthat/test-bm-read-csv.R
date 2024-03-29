test_that("read_csv setup", {
  defaults <- get_default_args(read_csv$setup)
  expect_named(defaults, c("source", "reader", "compression", "output_format"), ignore.order = TRUE)
})


test_that("read_csv benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      read_csv,
      source = c("nyctaxi_sample", "fanniemae_sample"),
      compression = "uncompressed",
      cpu_count = arrow::cpu_count()
    )
  )
})

wipe_results()
