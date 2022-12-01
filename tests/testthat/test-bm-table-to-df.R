test_that("table_to_df benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      table_to_df,
      source = "nyctaxi_sample",
      cpu_count = arrow::cpu_count()
    )
  )
})

wipe_results()