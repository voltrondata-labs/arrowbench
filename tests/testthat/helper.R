wipe_results <- function() unlink(test_path("results/"), recursive = TRUE)

expect_benchmark_run <- function(..., success = TRUE) {
  suppress_deparse_warning(
    # Capture the messages
    output <- capture.output(
      # Expect some console output
      expect_output(
        result <- eval(...)
      ),
      type = "message"
    )
  )

  expect_s3_class(result, "BenchmarkResults")

  # If we require success, then we should confirm that the `error` attribute of
  # each result is empty
  if (success) {
    # the calling handler, etc is all so that we can send _one_ instance of the
    # message output and not a bunch
    messaged <- FALSE
    withCallingHandlers(
      for (res in result$results) {
        expect_null(res$error)
      },
      error = function(e) {
        if (!messaged) {
          message(paste0(output, collapse = "\n"))
          messaged <<- TRUE
        }
        e
      }
    )
  }

}

suppress_deparse_warning <- function(...) {
  # surpress the deparse may be incomplete warnings which are a side-effect of
  # loadall + testing
  withCallingHandlers(
    ...,
    warning = function(w) {
      if (startsWith(conditionMessage(w), "deparse may be incomplete"))
        invokeRestart("muffleWarning")
    })
}


assert_benchmark_dataframe <- function(bm_df, benchmarks, parameters) {
  if (missing(parameters)) {
    parameters <- rep(list(NULL), length(benchmarks))
  }

  expect_s3_class(bm_df, c("BenchmarkDataFrame", "tbl", "tbl_df", "data.frame"))
  expect_true(all(c("name", "benchmark", "parameters") %in% names(bm_df)))
  expect_equal(nrow(bm_df), length(benchmarks))
  expect_equal(bm_df$name, vapply(benchmarks, function(x) x$name, character(1)))
  expect_equal(bm_df$benchmark, benchmarks)
  expect_equal(bm_df$parameters, parameters)
}

