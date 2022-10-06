test_that("array_altrep_materialization benchmark runs", {

  expect_benchmark_run(
    run_benchmark(
      array_altrep_materialization,
      source = "fanniemae_sample",
      altrep = TRUE,
      cpu_count = arrow::cpu_count()
    )
  )
})
