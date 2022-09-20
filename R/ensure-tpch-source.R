#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale_factor = 1) {
  stopifnot(datalogistik_available())

  scale_factor_str <- format(scale_factor, scientific = FALSE)

  file_metadata <- datalogistik_generate(paste0("-d='tpc-h' -f='parquet' -s=", scale_factor_str))

  tpch_files <- file_metadata$tables

  as.list(tpch_files)[order(names(tpch_files))]
}

ensure_tpch <- function(scale_factor = 1) {
  # let datalogistik handle caching
  generate_tpch(scale_factor)
}