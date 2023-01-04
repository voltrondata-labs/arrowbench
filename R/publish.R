#' Augment a benchmark result with collected defaults
#'
#' @param result An instance of [BenchmarkResult]
#'
#' @export
augment_result <- function(result) {
  stopifnot(
    benchconnect_available(),
    inherits(result, "BenchmarkResult")
  )

  tmp_in <- tempfile(fileext = '.json')
  result$write_json(tmp_in)

  command_res <-   processx::run(
    'benchconnect',
    args = c("augment", "result", "--path", tmp_in)
  )
  file.remove(tmp_in)
  BenchmarkResult$from_json(command_res$stdout)
}