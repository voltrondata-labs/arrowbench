#' Convert benchmark result object to a tidy data frame
#'
#' @param x list of benchmark results returned by [run_benchmark()]
#' @param row.names for generic consistency
#' @param optional for generic consistency
#' @param ... additional arguments passed to `.result_to_df()`. `packages` is
#' the only currently supported argument, which governs which package versions
#' to keep as columns in the result.
#' @return A data.frame suitable for analysis in R
#' @export
as.data.frame.arrowbench_results <- function(x, row.names = NULL, optional = FALSE, ...) {
  valid <- map_lgl(x, ~is.null(.$error))

  dplyr::bind_rows(lapply(x[valid], as.data.frame, ...))
}

#' @export
as.data.frame.arrowbench_result <- function(x, row.names = NULL, optional = FALSE, packages = "arrow", ...) {
  pkgs <- x$params$packages
  x$params$packages <- NULL
  for (p in packages) {
    x$params[[paste0("version_", p)]] <- pkgs[p, "version"]
  }
  out <- cbind(iteration = seq_len(nrow(x$result)), x$result, x$params)
  out$output <- x$output
  out
}