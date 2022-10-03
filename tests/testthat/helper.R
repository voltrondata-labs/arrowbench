library(jsonlite)

wipe_results <- function() unlink(test_path("results/"), recursive = TRUE)

expect_benchmark_run <- function(...) {
  suppress_deparse_warning(
    # Capture the messages
    capture.output(
      # Expect some console output
      expect_output(
        expect_s3_class(
          ...,
          "BenchmarkResults"
        )
      ),
      type = "message"
    )
  )
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

