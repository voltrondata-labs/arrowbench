

test_that("write_file benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      write_file,
      source = "nyctaxi_sample",
      file_type = c("parquet", "feather"),
      compression = c("uncompressed", "snappy", "lz4"),
      input_type = c("arrow_table", "data_frame"),
      cpu_count = arrow::cpu_count()
    ),
    "BenchmarkResults"
  )
})

wipe_results()
