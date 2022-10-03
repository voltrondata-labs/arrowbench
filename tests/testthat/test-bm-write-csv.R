

test_that("write_csv benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      write_csv,
      source = "nyctaxi_sample",
      writer = c("arrow", "data.table", "vroom", "readr", "base")
    )
  )
})

wipe_results()
