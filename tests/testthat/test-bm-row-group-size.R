test_that("row_group_size benchmark runs", {

  params <- default_params(row_group_size, chunk_size = list(NULL, 10000L, 100000L, 1000000L))

  expect_s3_class(
    run_benchmark(
      row_group_size,
      source = "fanniemae_sample",
      queries = "everything",
      chunk_size = list(NULL)
    ),
    "arrowbench_results"
  )
})
