test_that("row_group_size benchmark runs", {

  params <- get_default_parameters(row_group_size, chunk_size = list(NULL, 10000L, 100000L, 1000000L))

  expect_benchmark_run(
    run_benchmark(
      row_group_size,
      source = "fanniemae_sample",
      queries = "everything",
      chunk_size = list(1000L),
      cpu_count = arrow::cpu_count()
    )
  )
})

wipe_results()
