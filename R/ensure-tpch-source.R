#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale_factor = 1) {
  # Ensure that we have our custom duckdb that has the TPC-H extension built.
  ensure_custom_duckdb()

  duckdb_file <- tempfile()
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = duckdb_file))
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(duckdb_file)
  }, add = TRUE)
  DBI::dbExecute(con, paste0("CALL dbgen(sf=", scale_factor, ");"))

  out <- lapply(tpch_tables, function(name) {
    # TODO: use arrow-native when merges https://github.com/apache/arrow/pull/11032
    res <- DBI::dbSendQuery(con, paste0("SELECT * FROM ", name, ";"), arrow = TRUE)
    tab <- duckdb::duckdb_fetch_record_batch(res)$read_table()

    filename <- source_data_file(paste0(name, "_", format(scale_factor, scientific = FALSE), ".parquet"))
    arrow::write_parquet(tab, filename)
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