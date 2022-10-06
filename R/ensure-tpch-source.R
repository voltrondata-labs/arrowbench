#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

ensure_tpch <- function(scale_factor = 1) {
  ensure_source("tpc-h", format = "parquet", scale_factor = scale_factor)
}