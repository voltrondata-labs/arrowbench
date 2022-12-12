assert_benchmark_dataframe <- function(bm_df, benchmarks, parameters) {
  if (missing(parameters)) {
    parameters <- rep(list(NULL), length(benchmarks))
  }

  expect_s3_class(bm_df, c("BenchmarkDataFrame", "tbl", "tbl_df", "data.frame"))
  expect_true(all(c("name", "benchmark", "parameters") %in% names(bm_df)))
  expect_equal(nrow(bm_df), length(benchmarks))
  expect_equal(bm_df$name, vapply(benchmarks, function(x) x$name, character(1)))
  expect_equal(bm_df$benchmark, benchmarks)
  expect_equal(bm_df$parameters, parameters)
}


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
  param_list <- list(default_params(placebo), NULL)
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


test_that("default_params.BenchmarkDataFrame() can fill in params col", {
  bm_list <- list(placebo, placebo)
  bm_df <- BenchmarkDataFrame(bm_list)
  assert_benchmark_dataframe(bm_df, bm_list)

  bm_df_augmented <- default_params(bm_df)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, default_params))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, default_params(placebo))
    expect_gt(nrow(param_df), 0L)
  })

  # handle keyword args
  bm_df_augmented <- default_params(bm_df, duration = 1)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, default_params, duration = 1))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, default_params(placebo, duration = 1))
    expect_gt(nrow(param_df), 0L)
  })

  # handle partially-specified param lists
  bm_df <- BenchmarkDataFrame(bm_list, parameters = list(default_params(placebo, duration = 1), NULL))
  bm_df_augmented <- default_params(bm_df, duration = 1)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, default_params, duration = 1))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, default_params(placebo, duration = 1))
    expect_gt(nrow(param_df), 0L)
  })
})


test_that("run() dispatches and run.default() errors", {
  expect_error(
    run(1),
    "No method found for class `numeric`"
  )
})


test_that("run.BenchmarkDataFrame() works", {
  bm_list <- list(placebo, placebo)
  param_list <- list(
    default_params(
      placebo,
      error = list(NULL, "rlang::abort", "base::stop"),
      cpu_count = arrow::cpu_count()
    ),
    NULL
  )
  bm_df <- BenchmarkDataFrame(benchmarks = bm_list, parameters = param_list)

  bm_df_res <- run(bm_df)

  assert_benchmark_dataframe(
    bm_df_res,
    benchmarks = bm_list,
    parameters = list(param_list[[1]], default_params(placebo))
  )
  expect_true("results" %in% names(bm_df_res))
  purrr::walk2(bm_df_res$parameters, bm_df_res$results, function(parameters, results) {
    expect_s3_class(results, c("BenchmarkResults", "Serializable", "R6"))
    expect_equal(nrow(parameters), length(results$results))
    if ("error" %in% names(parameters)) {
      # param set with some cases that will error
      purrr::walk2(parameters$error, results$results, function(err, res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        if (is.null(err)) {
          # passing case
          expect_null(res$error)
          expect_gt(res$stats$data[[1]], 0)
        } else {
          # erroring case
          expect_false(is.null(res$error))
        }
      })
    } else {
      # param set with no cases that will error (includes defaults)
      purrr::walk(results$results, function(res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        expect_null(res$error)
        expect_gt(res$stats$data[[1]], 0)
      })
    }
  })
})