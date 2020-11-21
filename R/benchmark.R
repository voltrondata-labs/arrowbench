#' Define a Benchmark
#'
#' @section Evaluation:
#' A `Benchmark` is evaluated something like:
#'
#' ```
#' ctx <- new.env()
#' bm$setup(ctx, param1 = "value", param2 = "value")
#' for (i in seq_len(n_iter)) {
#'   bm$before_each(ctx)
#'   measure(bm$run(ctx))
#'   bm$after_each(ctx)
#' }
#' bm$teardown(ctx)
#' ```
#'
#' @param setup
#' @param before_each
#' @param run
#' @param after_each
#' @param teardown
#' @param valid_params
#' @param ...
#'
#' @return A `Benchmark` object containing these functions
#' @export
Benchmark <- function(setup = function(ctx, ...) {},
                      before_each = function(ctx) {},
                      run = function(ctx) {},
                      after_each = function(ctx) {},
                      teardown = function(ctx) {},
                      valid_params = function(params) params,
                      ...) {
  structure(
    list(
      setup = setup,
      before_each = before_each,
      run = run,
      after_each = after_each,
      teardown = teardown,
      valid_params = valid_params,
      ...),
    class = "Benchmark"
  )
}

default_params <- function(bm, ...) {
  # This takes the expansion of the default parameters in the function signature
  # perhaps restricted by the ... params
  params <- modifyList(get_default_args(bm$setup), list(...))
  params$stringsAsFactors <- FALSE
  out <- do.call(expand.grid, params)

  if (!is.null(bm$valid_params)) {
    out <- bm$valid_params(out)
  }
  out
}