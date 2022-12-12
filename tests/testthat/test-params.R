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
