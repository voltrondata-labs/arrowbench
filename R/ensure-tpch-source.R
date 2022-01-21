#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale_factor = 1) {
  # Ensure that we have our custom duckdb that has the TPC-H extension built.
  ensure_custom_duckdb()

  duckdb_file <- tempfile()
  on.exit(unlink(duckdb_file, recursive = TRUE))

  # generate the tables
  query_custom_duckdb(
    paste0("CALL dbgen(sf=", scale_factor, ");"),
    dbdir = duckdb_file
  )

  # write each table to paruqet
  out <- lapply(tpch_tables, function(name) {
    filename <- source_data_file(paste0(name, "_", format(scale_factor, scientific = FALSE), ".parquet"))
    query <- paste0("SELECT * FROM ", name, ";")
    export_custom_duckdb(query, filename, dbdir = duckdb_file)

    filename
  })

  set_names(out, tpch_tables)
}

#' @importFrom rlang set_names
ensure_tpch <- function(scale_factor = 1) {
  ensure_source_dirs_exist()

  filenames <- paste0(paste(tpch_tables, format(scale_factor, scientific = FALSE), sep="_"), ".parquet")

  # Check for places this file might already be and return those.
  cached_files <- map(filenames, data_file)
  if (all(!map_lgl(cached_files, is.null))) {
    # if the file is in our temp storage or source storage, go for it there.
    return(set_names(cached_files, nm = tpch_tables))
  }

  # generate it
  generate_tpch(scale_factor)
}