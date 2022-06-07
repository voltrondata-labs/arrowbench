test_that("R6.1 classes inherit properly", {
  SumClass <- R6Point1Class(
    classname = "SumClass",
    static = list(sum = sum, x = 1:100)
  )

  sum_class <- SumClass$new()
  expect_s3_class(sum_class, "SumClass")
  expect_identical(SumClass$sum, sum)

  SumOtherClass <- R6Point1Class(
    classname = "SumOtherClass",
    inherit = SumClass
  )

  sum_other_class <- SumOtherClass$new()
  expect_s3_class(sum_other_class, "SumOtherClass")
  expect_identical(SumOtherClass$sum, sum)

  expect_equal(SumOtherClass$sum(SumOtherClass$x), 5050L)
})


test_that("inherited serialization/deserialization methods work", {
  res <- BenchmarkResult$new(
    name = "fake",
    result = data.frame(time = 0, status = "superfast", stringsAsFactors = FALSE),
    params = list(speed = "lightning"),
    tags = c(is_real = FALSE)
  )

  # sanity
  expect_s3_class(res, "BenchmarkResult")
  expect_equal(res$name, "fake")

  # roundtrips
  expect_equal(res$json, BenchmarkResult$from_json(res$json)$json)
  expect_equal(res$list, BenchmarkResult$from_list(res$list)$list)

  temp <- tempfile(fileext = '.json')
  res$write_json(temp)
  expect_equal(res$json, BenchmarkResult$read_json(temp)$json)
  file.remove(temp)
})

test_that("S3 methods work", {
  res <- BenchmarkResult$new(
    name = "fake",
    result = data.frame(time = 0, status = "superfast", stringsAsFactors = FALSE),
    params = list(speed = "lightning"),
    tags = c(is_real = FALSE)
  )

  expect_equal(as.character(res), res$json)
  expect_equal(as.list(res), res$list)

  expect_equal(as.data.frame(res), res$to_dataframe())
  expect_equal(
    as.data.frame(res),
    structure(
      list(iteration = 1L, time = 0, status = "superfast", speed = "lightning"),
      row.names = c(NA, -1L), class = c("tbl_df", "tbl", "data.frame"),
      name = "fake", tags = c(is_real = FALSE)
    )
  )

  expect_equal(get_params_summary(res), res$params_summary)
  expect_equal(
    get_params_summary(res),
    structure(
      list(speed = "lightning", did_error = FALSE),
      row.names = c(NA, -1L), class = c("tbl_df", "tbl", "data.frame")
    )
  )
})
