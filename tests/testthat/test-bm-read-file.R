test_that("read_file validation", {
  # read_file has a few combinations in its default arguments that aren't valid
  read_file_no_validate <- read_file
  read_file_no_validate$valid_params <- NULL

  params_no_validate <- default_params(read_file_no_validate)

  params <- default_params(read_file)

  expect_lt(nrow(params), nrow(params_no_validate))

  # specifically feather+snappy is not a possibility
  expect_identical(
    nrow(params[params$file_type == "feather" & params$compression == "snappy", ]),
    0L
  )
})

for (format in c("parquet", "feather", "ndjson", "fst")) {
  if (format == "parquet") {
    compression <- c("uncompressed", "snappy", "lz4")
  } else {
    compression <- "uncompressed"
  }

  test_that(paste0("read_file benchmark works for ", format), {
    if (format %in% c("feather", "ndjson", "fst")) {
      skip("Formats not yet supported by datalogistik")
    }
    expect_benchmark_run(
      run_benchmark(
        read_file,
        source = "nyctaxi_sample",
        file_type = format,
        compression = compression,
        output_type = c("arrow_table", "data_frame"),
        cpu_count = arrow::cpu_count()
      )
    )
  })
}


wipe_results()
