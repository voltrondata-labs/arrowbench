
ensure_custom_duckdb <- function(lib = custom_duckdb_lib_dir(), install = TRUE,
                                 quiet = FALSE) {
  result <- tryCatch({
      query_custom_duckdb(
        "select scale_factor, query_nr from tpch_answers() LIMIT 1;",
        lib = lib
      )
    },
    error = function(e) {
      error_is_from_us <- grepl(
        paste0(c(
          "(name tpch_answers is not on the catalog)",
          "(name tpch_answers does not exist)",
          "(there is no package called 'duckdb')",
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

  if (install) {
    install_custom_duckdb(lib, quiet = quiet)
    result <- try(
      ensure_custom_duckdb(lib, install = FALSE, quiet = quiet),
      silent = TRUE
    )

    if (!inherits(result, "try-error")) {
      return(invisible(NULL))
    }
  }

  stop(
    paste(
      "Custom duckdb build with TPC-H extension could not be loaded",
      if (install) "and could not be installed." else "and `install = FALSE`"
    )
  )
}

query_custom_duckdb <- function(sql, dbdir = ":memory:", lib = custom_duckdb_lib_dir()) {
  fun <- function(sql, dbdir, lib) {
    # don't load duckdb from anything except `lib` and error otherwise
    # because the subprocess may have duckdb in a default, site, or user lib
    if (!requireNamespace("duckdb", lib.loc = lib, quietly = TRUE)) {
      stop(
        sprintf("there is no package called 'duckdb'"),
        call. = FALSE
      )
    }

    con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    DBI::dbExecute(con, "LOAD tpch;")
    DBI::dbGetQuery(con, sql)
  }

  callr::r(fun, list(sql, dbdir, lib), libpath = c(lib, .libPaths()))
}

export_custom_duckdb <- function(sql, sink, dbdir = ":memory:", lib = custom_duckdb_lib_dir()) {
  fun <- function(sql, sink, dbdir, lib) {
    # don't load duckdb from anything except `lib` and error otherwise
    # because the subprocess may have duckdb in a default, site, or user lib
    if (!requireNamespace("duckdb", lib.loc = lib, quietly = TRUE)) {
      stop(
        sprintf("there is no package called 'duckdb'"),
        call. = FALSE
      )
    }

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

  callr::r(fun, list(sql, sink, dbdir, lib), libpath = c(lib, .libPaths()))
}

# This installs into a custom library so that it is isolated from duckdb under test
# While this was much more important when we needed to (re)build duckdb to enable the tpc-h
# extension, it's still helpful today to isolate duckdb changes from the ability to get answers
install_custom_duckdb <- function(lib = custom_duckdb_lib_dir(), force = TRUE, quiet = FALSE) {
  if (!quiet) {
    message(
      paste0(
        "Installing duckdb with the ability to generate TPC-H datasets ",
        "to custom library \n'", lib, "'"
      )
    )
  }

  # `build = FALSE` so that the duckdb cpp source is available when the R package
  # is compiling itself
  fun <- function(lib) {
    if (!dir.exists(lib)) {
      dir.create(lib, recursive = TRUE)
    }

    remotes::install_cran("duckdb", lib = lib, force = force)
    con <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(con, "INSTALL tpch; LOAD tpch;")
    DBI::dbDisconnect(con)
  }

  callr::r(fun, list(lib), libpath = c(lib, .libPaths()), show = !quiet)
}

custom_duckdb_lib_dir <- function() {
  lib_dir("custom_duckdb")
}
