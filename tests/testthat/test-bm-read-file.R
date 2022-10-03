test_that("read_file validation", {
  # read_file has a few combinations in its default arguments that aren't valid
  read_file_no_validate <- read_file
  read_file_no_validate$valid_params <- NULL

  params_no_validate <- default_params(read_file_no_validate)

  params <- default_params(read_file)

  expect_lt(nrow(params), nrow(params_no_validate))

  # specifically feather+snappy is not a possibility
  expect_identical(
    nrow(params[params$format == "feather" & params$compression == "snappy", ]),
    0L
  )
})


test_that("read_file benchmark works", {
  expect_benchmark_run(
    run_benchmark(
      read_file,
      source = "nyctaxi_sample",
      format = c("parquet", "feather"),
      compression = c("uncompressed", "snappy", "lz4"),
      output = c("arrow_table", "data_frame")
    )
  )
})

wipe_results()
