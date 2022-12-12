test_that("BenchmarkDataFrame can be instantiated", {
  for (bm_list in list(
    list(placebo),
    list(placebo, placebo),
    list(a = placebo, b = placebo)
  )) {
    bm_df <- BenchmarkDataFrame(benchmarks = bm_list)
    assert_benchmark_dataframe(bm_df, benchmarks = bm_list)
  }

  bm_list <- list(placebo, placebo)
  param_list <- list(get_default_parameters(placebo), NULL)
  bm_df <- BenchmarkDataFrame(benchmarks = bm_list, parameters = param_list)
  assert_benchmark_dataframe(bm_df, benchmarks = bm_list, parameters = param_list)

  expect_error(
    BenchmarkDataFrame(1),
    "All elements of `benchmarks` are not of class `Benchmark`!"
  )
})


test_that("format.BenchmarkDataFrame() works", {
  bm_df <- BenchmarkDataFrame(benchmarks = list(placebo))
  expect_output(print(bm_df), "# <BenchmarkDataFrame>")
})


test_that("`get_package_benchmarks()` works", {
  bm_df <- get_package_benchmarks()
  assert_benchmark_dataframe(bm_df = bm_df, benchmarks = bm_df$benchmark)
  expect_gt(nrow(bm_df), 0L)
  # currently `any()` because `wide-dataframe` is actually a Python benchmark,
  # but is still listed in arrow-benchmarks-ci in R. If removed, change to `all()`.
  expect_true(any(URSA_I9_9960X_R_BENCHMARK_NAMES %in% bm_df$name))
})
