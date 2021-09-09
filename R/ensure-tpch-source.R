#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale = 1) {
  # Ensure that we have our custom duckdb that has the TPC-H extension built.
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, paste0("CALL dbgen(sf=", scale, ");"))

  out <- lapply(tpch_tables, function(name) {
    # TODO: use arrow-native when merges https://github.com/apache/arrow/pull/11032
    res <- DBI::dbSendQuery(con, paste0("SELECT * FROM ", name, ";"), arrow = TRUE)
    tab <- duckdb::duckdb_fetch_record_batch(res)$read_table()

    # Convert all decimals to floats, can remove when ARROW-13966 is merged
    decimals <- colnames(tab)[purrr::map_lgl(tab$schema$fields, ~inherits(.x$type, "DecimalType"))]
    for (nm in decimals) {
      tab[[nm]] <- tab[[nm]]$cast(arrow::float64())
    }

    filename <- source_data_file(paste0(name, "_", format(scale, scientific = FALSE), ".parquet"))
    arrow::write_parquet(tab, filename)
    filename
  })

  set_names(out, tpch_tables)
}

#' @importFrom rlang set_names
ensure_tpch <- function(scale = 1) {
  ensure_source_dirs_exist()

  filenames <- paste0(paste(tpch_tables, format(scale, scientific = FALSE), sep="_"), ".parquet")

  # Check for places this file might already be and return those.
  cached_files <- map(filenames, data_file)
  if (all(!map_lgl(cached_files, is.null))) {
    # if the file is in our temp storage or source storage, go for it there.
    return(set_names(cached_files, nm = tpch_tables))
  }

  # generate it
  generate_tpch(scale)
}