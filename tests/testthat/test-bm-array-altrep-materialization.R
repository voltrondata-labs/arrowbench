test_that("array_altrep_materialization benchmark runs", {

  expect_s3_class(
    run_benchmark(
      array_altrep_materialization,
      source = "type_integers",
      altrep = TRUE
    ),
    "BenchmarkResults"
  )
})
