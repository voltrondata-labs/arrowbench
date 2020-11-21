#' Convert benchmark result object to a tidy data frame
#'
#' @param results list of benchmark results returned by [run_benchmark()]
#' @param ... additional arguments passed to `.result_to_df()`. `packages` is
#' the only currently supported argument, which governs which package versions
#' to keep as columns in the result.
#' @return A data.frame suitable for analysis in R
#' @export
as.data.frame.conbench_results <- function(results, ...) {
  valid <- map_lgl(results, ~is.null(.$error))

  do.call(rbind, lapply(results[valid], as.data.frame, ...))
}

#' @export
as.data.frame.conbench_result <- function(x, packages = "arrow", ...) {
  pkgs <- x$params$packages
  x$params$packages <- NULL
  for (p in packages) {
    x$params[[paste0("version_", p)]] <- pkgs[p, "version"]
  }
  cbind(iteration = seq_len(nrow(x$result)), x$result, x$params)
}