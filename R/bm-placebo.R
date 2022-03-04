#' Placebo benchmark for testing
#'
#' @section Parameters:
#' * `duration` the duration for the benchmark to take
#' * `error_type` `NULL` to cause no error, `"rlang::abort"` to use rlang's
#' `abort` and any other string (including `"base"`) will use base's `stop`
#'
#' @keywords internal
placebo <- Benchmark("placebo",
  setup = function(duration = 0.01, error_type = NULL, output_type = NULL, grid = TRUE) {
    BenchEnvironment(placebo_func = function() {
      if (!is.null(output_type)) {
        msg <- "here's some output"
        if (output_type == "message") {
          message("A message: ", msg)
        } else if (output_type == "warning") {
          warning("A warning:", msg)
        } else if (output_type == "cat") {
          cat("A cat:", msg)
        }
      }

      if (!is.null(error_type)) {
        msg <- "something went wrong (but I knew that)"
        if (error_type == "rlang::abort") {
          rlang::abort(msg)
        }
        stop(msg)
      }
      Sys.sleep(duration)
      })
  },
  before_each = TRUE,
  run = {
    placebo_func()
  },
  after_each = TRUE,
  valid_params = function(params) {
    params
  },
  packages_used = function(params) {
    "base"
  }
)
