

test_that("write_csv benchmark works", {
  expect_s3_class(
    run_benchmark(
      write_csv,
      source = "nyctaxi_sample",
      writer = c("arrow", "data.table", "vroom", "readr", "base")
      ), "arrowbench_results"
  )
})

wipe_results()
