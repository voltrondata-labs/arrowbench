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
