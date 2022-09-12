expected_filenames <- as.list(setNames(
  paste0(tpch_tables, ".parquet"),
  nm = tpch_tables
))


test_that("can generate a small dataset", {
  tpch_files <- ensure_tpch(0.01)
  expect_identical(
    lapply(tpch_files, basename),
    expected_filenames
  )
})

test_that("cached data gets used", {
  mockery::stub(ensure_tpch, 'datalogistik_generate', function(params) {
    command <- paste("datalogistik generate", paste(params, collapse = " "))
    output <- system2(command, stdout = TRUE, stderr = TRUE)
    expect_match(paste(output, collapse = TRUE), "Found cached dataset")
    jsonlite::fromJSON(output[length(output)])
  })

  tpch_files <- ensure_tpch(0.01)
  expect_identical(
    lapply(tpch_files, basename),
    expected_filenames
  )
})

test_that("and ensure gets the same thing", {
  tpch_files <- ensure_source("tpch", scale_factor = 0.01)
  expect_identical(
    lapply(tpch_files, basename),
    expected_filenames
  )
})
