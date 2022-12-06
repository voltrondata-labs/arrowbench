test_that("dataset_taxi_2013 exists", {
  defaults <- get_default_args(dataset_taxi_2013$setup)

  expect_named(defaults, c("dataset", "query"))
  expect_equal(
    defaults$query,
    c("basic", "payment_type_crd", "small_no_files", "dims")
  )
})

test_that("dataset_taxi_2013 runs on sample data", {
  expect_benchmark_run(
    res <- run_benchmark(dataset_taxi_2013, dataset = "taxi_2013_sample", cpu_count = arrow::cpu_count())
  )

  lapply(res$optional_benchmark_info$results, function(result) {
    expect_s3_class(result, "BenchmarkResult")
    expect_gte(result$result$real, 0)
  })
})
