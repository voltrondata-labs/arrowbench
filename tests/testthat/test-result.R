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
    run_name = "fake_run",
    tags = c(is_real = FALSE),
    optional_benchmark_info = list(
      name = "fake",
      result = data.frame(time = 0, status = "superfast", stringsAsFactors = FALSE),
      params = list(speed = "lightning")
    )
  )

  # sanity
  expect_s3_class(res, "BenchmarkResult")
  expect_equal(res$run_name, "fake_run")

  # roundtrips
  expect_equal(res$json, BenchmarkResult$from_json(res$json)$json)
  expect_equal(res$list, BenchmarkResult$from_list(res$list)$list)

  temp <- tempfile(fileext = '.json')
  res$write_json(temp)
  expect_equal(res$json, BenchmarkResult$read_json(temp)$json)
  file.remove(temp)
})

test_that("S3 methods work", {
  github <- list(
    repository = "https://github.com/conchair/conchair",
    commit = "2z8c9c49a5dc4a179243268e4bb6daa5",
    pr_number = 47L
  )
  run_reason <- "mocked-arrowbench-unit-test"
  run_name <- paste(run_reason, github$commit, sep = ": ")
  host_name <- "fake-computer"

  withr::with_envvar(
    c(
      CONBENCH_PROJECT_REPOSITORY = github$repository,
      CONBENCH_PROJECT_PR_NUMBER = github$pr_number,
      CONBENCH_PROJECT_COMMIT = github$commit,
      CONBENCH_MACHINE_INFO_NAME = host_name
    ),
    {
      res <- BenchmarkResult$new(
        run_name = run_name,
        run_reason = run_reason,
        tags = c(is_real = FALSE),
        optional_benchmark_info = list(
          name = "fake",
          result = data.frame(time = 0, status = "superfast", stringsAsFactors = FALSE),
          params = list(speed = "lightning")
        )
      )
    }
  )

  expect_equal(as.character(res), res$json)
  expect_equal(as.list(res), res$list)

  expect_equal(as.data.frame(res), res$to_dataframe())
  expect_equal(
    as.data.frame(res),
    structure(
      list(iteration = 1L, time = 0, status = "superfast", speed = "lightning"),
      row.names = c(NA, -1L),
      class = c("tbl_df", "tbl", "data.frame"),
      run_name = run_name,
      run_reason = run_reason,
      github = github,
      timestamp = res$timestamp,
      tags = c(is_real = FALSE)
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
