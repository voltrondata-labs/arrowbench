#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

#' Generate tpch data
#'
#' Generate tpch data at a given scale factor. By default,
#' data is output relative to the current working directory. However,
#' you can set the environment variable `ARROWBENCH_DATA_DIR` to
#' point to another directory. Setting this environment variable has
#' the advantage of being a central location for general usage. Running
#' this function will install a custom version of duckdb in an `r_libs`
#' directory, relative to the directory specified by the environment
#' variable `ARROWBENCH_LOCAL_DIR`. When running this function for the first time you will
#' see significant output from that installation process. This is normal.
#'
#' @param scale_factor a relative measure of the size of data in gigabytes.
#'
#' @export
ensure_tpch <- function(scale_factor = 1) {
  ensure_source("tpc-h", format = "parquet", scale_factor = scale_factor)
}
