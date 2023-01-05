test_that("get_default_parameters.BenchmarkDataFrame() can fill in params col", {
  bm_list <- list(placebo, placebo)
  bm_df <- BenchmarkDataFrame(bm_list)
  assert_benchmark_dataframe(bm_df, bm_list)

  bm_df_augmented <- get_default_parameters(bm_df)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, get_default_parameters))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, get_default_parameters(placebo))
    expect_gt(nrow(param_df), 0L)
  })

  # handle keyword args
  bm_df_augmented <- get_default_parameters(bm_df, duration = 1)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, get_default_parameters, duration = 1))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, get_default_parameters(placebo, duration = 1))
    expect_gt(nrow(param_df), 0L)
  })

  # handle partially-specified param lists
  bm_df <- BenchmarkDataFrame(bm_list, parameters = list(get_default_parameters(placebo, duration = 1), NULL))
  bm_df_augmented <- get_default_parameters(bm_df, duration = 1)
  assert_benchmark_dataframe(bm_df_augmented, bm_list, lapply(bm_list, get_default_parameters, duration = 1))
  lapply(bm_df_augmented$parameters, function(param_df) {
    expect_s3_class(param_df, "data.frame")
    expect_equal(param_df, get_default_parameters(placebo, duration = 1))
    expect_gt(nrow(param_df), 0L)
  })
})
