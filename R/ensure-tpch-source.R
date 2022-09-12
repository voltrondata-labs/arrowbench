#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale_factor = 1) {
  stopifnot(datalogistik_available())

  scale_factor_str <- format(scale_factor, scientific = FALSE)

  file_metadata <- datalogistik_generate(paste0("-d='tpc-h' -f='parquet' -s=", scale_factor_str))

  source_dir <- file_metadata$path

  tpch_files <- file.path(file_metadata$path, file_metadata$files)
  names(tpch_files) <- sub('\\..*$', '', file_metadata$files)

  as.list(tpch_files)[order(names(tpch_files))]
}

ensure_tpch <- function(scale_factor = 1) {
  # let datalogistik handle caching
  generate_tpch(scale_factor)
}