

test_that("write_file benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      write_file,
      source = "nyctaxi_sample",
      format = c("parquet", "feather"),
      compression = c("uncompressed", "snappy", "lz4"),
      input = c("arrow_table", "data_frame")
    )
  )
})

wipe_results()
