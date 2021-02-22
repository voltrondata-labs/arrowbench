#' Placebo benchmark for testing
#'
#' @section Parameters:
#' * `duration` the duration for the benchmark to take
#'
placebo <- Benchmark("placebo",
  setup = function(duration = 0.01, grid = TRUE, ...) {
    BenchEnvironment(placebo_func = function() {Sys.sleep(duration)})
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
