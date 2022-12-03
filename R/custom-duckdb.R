ensure_custom_duckdb <- function() {
  result <- tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "LOAD tpch;")
    DBI::dbGetQuery(con, "select scale_factor, query_nr from tpch_answers() LIMIT 1;")
  },
  error = function(e) {
    error_is_from_us <- grepl(
      paste0(c(
        "(name tpch_answers is not on the catalog)",
        "(name tpch_answers does not exist)",
        "(tpch.duckdb_extension\" not found)"
      ),
      collapse = "|"
      ),
      conditionMessage(e)
    )

    if (error_is_from_us) {
      NULL
    } else {
      rlang::abort(
        "An unexpected error occured whilst querying TPC-H enabled duckdb",
        parent = e
      )
    }
  }
  )

  # Check that the result has a query in it
  if (identical(result$query_nr, 1L)) {
    return(invisible(NULL))
  }


  install_duckdb_tpch()
  result <- try(
    ensure_custom_duckdb(),
    silent = FALSE
  )

  if (!inherits(result, "try-error")) {
    return(invisible(NULL))
  }

  stop("Could not load the DuckDB TPC-H extension.")
}

query_custom_duckdb <- function(sql, dbdir = ":memory:") {
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "LOAD tpch;")
  DBI::dbGetQuery(con, sql)
}

export_custom_duckdb <- function(sql, sink, dbdir = ":memory:") {
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "LOAD tpch;")
  res <- DBI::dbSendQuery(con, sql, arrow = TRUE)

  # this could be streamed in the future when the parquet writer
  # in R supports streaming
  reader <- duckdb::duckdb_fetch_record_batch(res)
  table <- reader$read_table()
  arrow::write_parquet(table, sink)

  sink
}

install_duckdb_tpch <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con))
  DBI::dbExecute(con, "INSTALL tpch; LOAD tpch;")
}
