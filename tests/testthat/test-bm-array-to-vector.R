test_that("array_to_vector benchmark runs", {

  expect_benchmark_run(
    run_benchmark(
      array_to_vector,
      source = "nyctaxi_sample",
      cpu_count = arrow::cpu_count()
    )
  )
})
