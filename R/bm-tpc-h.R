#' Benchmark TPC-H queries
#'
#' @section Parameters:
#' * `engine` One of `c("arrow", "duckdb", "dplyr")`
#' * `query_id` integer, 1-22
#' * `format` One of `c("parquet", "feather", "native")`
#' * `scale_factor` Scale factor to use for data generation (e.g. 0.1, 1, 10, 100)
#' * `memory_map` Should memory mapping be used when reading a file in? (only
#'   applicable to arrow, native. `FALSE` will result in the file being explicitly
#'   read into memory before the benchmark)
#' * `output` the format of the output (either `"data_frame"` (default) or `"arrow_table"`)
#' * `chunk_size` a size of row groups to aim for in parquet or feather files (default:
#'    NULL is the default for `arrow:write_parquet()` or `arrow::write_feather()`)
#'
#' @importFrom waldo compare
#' @export
tpc_h <- Benchmark("tpc_h",
  setup = function(engine = "arrow",
                   query_id = 1:22,
                   format = c("native", "parquet"),
                   scale_factor = c(1, 10),
                   memory_map = FALSE,
                   output = "data_frame",
                   chunk_size = NULL) {
    # engine defaults to arrow
    engine <- match.arg(engine, c("arrow", "duckdb", "duckdb_sql", "dplyr"))
    # input format
    format <- match.arg(format, c("parquet", "feather", "native"))
    # query_id defaults to 1 for now
    stopifnot(
      "query_id must be an int" = query_id %% 1 == 0,
      "query_id must 1-22" = query_id >= 1 & query_id <= 22
    )
    # output format
    output <- match.arg(output, c("arrow_table", "data_frame"))

    library("dplyr", warn.conflicts = FALSE)


    # for most engines, we want to use collect() (since arrow_table isn't an
    # output option)
    collect_func <- collect

    if (output == "data_frame") {
      collect_func <- collect
    } else if (output == "arrow_table") {
      collect_func <- compute
    }

    # we pass a connection around for duckdb, but not others
    con <- NULL

    if (engine %in% c("duckdb", "duckdb_sql")) {
      # we use this connection both to populate views/tables
      con <- DBI::dbConnect(duckdb::duckdb())

      # set parallelism for duckdb
      DBI::dbExecute(con, paste0("PRAGMA threads=", getOption("Ncpus")))
    }

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      input_func = get_input_func(
        engine = engine,
        scale_factor = scale_factor,
        query_id = query_id,
        format = format,
        con = con,
        memory_map = memory_map,
        chunk_size = chunk_size
      ),
      query = get_query_func(query_id, engine),
      engine = engine,
      con = con,
      scale_factor = scale_factor,
      query_id = query_id,
      collect_func = collect_func
    )
  },
  # delete the results before each iteration
  before_each = {
    result <- NULL
  },
  # the benchmark to run
  run = {
    result <- query(input_func, collect_func, con)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    # If the scale_factor is < 1, duckdb has the answer
    if (scale_factor %in% c(0.01, 0.1, 1, 10)) {
      answer <- tpch_answer(scale_factor, query_id)

      # the result is sometimes a data.frame, turn into a tibble for printing
      # purposes
      result <- dplyr::as_tibble(result)

      # TODO: different tolerances for different kinds of columns?
      # > For ratios, results r must be within 1% of the query validation output
      # data v when rounded to the nearest 1/100th. That is, 0.99*v<=round(r,2)<=1.01*v.
      # > For results from AVG aggregates, the resulting values r must be within 1%
      # of the query validation output data when rounded to the nearest 1/100th
      # > For results from SUM aggregates, the resulting values must be within
      # $100 of the query validation output data.
      all_equal_out <- waldo::compare(result, answer, tolerance = 0.01)

      if (length(all_equal_out) != 0) {
        warning(paste0("\n", all_equal_out, "\n"))
        stop("The answer does not match")
      }
    } else {
      warning("There is no validation for scale_factors other than 0.01, 0.1, 1, and 10. Be careful with these results!")
    }

    # clear the result
    result <- NULL
  },
  teardown = {
    if (!is.null(con)) {
      DBI::dbDisconnect(con, shutdown = TRUE)
    }
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    # only try feather with arrow
    drop <- ( params$engine != "arrow" & params$format == "feather" ) |
      # only try arrow_table with arrow
      ( params$engine != "arrow" & params$output == "arrow_table" ) |
      # only try memory_map with arrow
      ( params$engine != "arrow" & params$memory_map == TRUE) |
      # don't try native with dplyr
      # TODO: do this?
      ( params$engine == "dplyr" & params$format == "native" )
    params[!drop,]
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    out <- c(params$engine, "dplyr", "lubridate")
    if ("duckdb" %in% params$engine) {
      out <- c(out, "dbplyr")
    }
    out
  }
)

#' For extracting table names from TPC-H queries
#'
#' This searches a function for all references of `input_func(...)` and returns
#' the contents of `...`
#'
#' @param query_func a function containing a dplyr pipeline
#'
#' @return all references inside of `input_func(...)`, collapsed
#'
#' @export
tables_refed <- function(query_func) {
  unlist(find_input_func(body(query_func)), use.names = FALSE)
}

find_input_func <- function(func) {
  if (is.call(func)) {
    if (func[[1]] == "input_func") {
      return(func[[2]])
    } else {
      lapply(func, find_input_func)
    }
  }
}

#' Get an input function for a table
#'
#' This returns a function which will return a table reference with the specified
#' parameters
#'
#' @param engine which engine to use
#' @param scale_factor what scale factor to reference
#' @param query_id which query is being used
#' @param format which format
#' @param compression which compression to use (default: "uncompressed")
#' @param con a connection
#' @param memory_map should the file be memory mapped? (only relevant for the "native" format with Arrow)
#' @param chunk_size what chunk_size should be used with the source files? (default: NULL, the default for the file format)
#'
#' @export
get_input_func <- function(engine,
                           scale_factor,
                           query_id,
                           format,
                           compression = "uncompressed",
                           con = NULL,
                           memory_map = FALSE,
                           chunk_size = NULL) {
  # ensure that we have the _base_ tpc-h files (in parquet)
  tpch_files <- ensure_source("tpch", scale_factor = scale_factor)

  # find only the tables that are needed to process
  tpch_tables_needed <- tables_refed(tpc_h_queries[[query_id]])

  if (engine == "arrow") {
    # ensure that we have the right kind of files available
    # but for native, make sure we have a feather file, and we will read that
    # in to memory before the benchmark (below)
    format_to_convert <- format
    if (format == "native") {
      format_to_convert <- "feather"
    }

    tpch_files <- vapply(
      tpch_files[tpch_tables_needed],
      ensure_format,
      FUN.VALUE = character(1),
      format = format_to_convert,
      compression = compression,
      chunk_size = chunk_size
    )

    # specify readers for each format
    if (format == "parquet") {
      input_functions <- function(name) {
        file <- tpch_files[[name]]
        return(arrow::open_dataset(file, format = "parquet"))
      }
    } else if (format == "feather") {
      input_functions <- function(name) {
        file <- tpch_files[[name]]
        return(arrow::open_dataset(file, format = "feather"))
      }
    } else if (format == "native") {
      # native is different: read the feather file in first, and pass the table
      tab <- list()
      for (name in names(tpch_files)) {
        tab[[name]] <- arrow::read_feather(
          tpch_files[[name]],
          as_data_frame = FALSE,
          mmap = memory_map
        )
      }
      input_functions <- function(name) {
        return(tab[[name]])
      }
    }
  } else if (engine %in% c("duckdb", "duckdb_sql")) {
    input_functions <- function(name) {
      return(dplyr::tbl(con, name))
    }

    for (name in tpch_tables_needed) {
      file <- path.expand(tpch_files[[name]])

      # have to create a VIEW in order to reference it by name
      # This view is the most accurate comparison to Arrow, however it will
      # penalize duckdb since AFAICT `parquet_scan` is not parallelized and
      # ends up being the bottleneck
      if (format == "parquet") {
        sql_query <- paste0("CREATE OR REPLACE VIEW ", name, " AS SELECT * FROM parquet_scan('", file, "');")
      } else if (format == "native") {
        sql_query <- paste0("CREATE TABLE IF NOT EXISTS ", name, " AS SELECT * FROM parquet_scan('", file, "');")
      }

      DBI::dbExecute(con, sql_query)
    }

  } else if (engine == "dplyr") {
    requireNamespace("lubridate")
    if (format == "parquet") {
      input_functions <- function(name) {
        file <- tpch_files[[name]]
        return(arrow::read_parquet(file, as_data_frame = TRUE))
      }
    }
  }

  input_functions
}


#' Get a query function that will run a specific TPC-H query
#'
#' @param query_id which query to get?
#' @param engine which engine to use (all options return a dplyr-based query,
#' with the except of `"duckdb_sql"` which returns a SQL-based query)
#'
#' @export
get_query_func <- function(query_id, engine = NULL) {

  if (!is.null(engine) && engine == "duckdb_sql") {
    # If we are using the SQL engine, then we need to get the SQL
    return(get_sql_query_func(query_id))
  } else {
    # For all other engines, use the dplyr in tpc_h_queryes
    return(tpc_h_queries[[query_id]])
  }
}

#' Get a TPC-H answer
#'
#' @param scale_factor scale factor (possible values: `c(0.01, 0.1, 1, 10)`)
#' @param query_id Id of the query (possible values: 1-22)
#' @param source source of the answer (default: "arrowbench"), "duckdb" can
#' return answers for scale_factor 1.
#'
#' @return the answer, as a data.frame
#' @export
tpch_answer <- function(scale_factor, query_id, source = c("arrowbench", "duckdb")) {
  source <- match.arg(source)

  if (source == "arrowbench") {
    scale_factor_string <- format(scale_factor, scientific = FALSE)
    answer_file <- system.file(
      "tpch",
      "answers",
      paste0("scale-factor-", scale_factor_string),
      paste0("tpch-q", sprintf("%02i", query_id), "-sf", scale_factor_string, ".parquet"),
      package = "arrowbench"
    )

    if (!file.exists(answer_file)) {
      stop(
        "The answer file (looking for ",
        file.path(
          "arrowbench",
          "tpch",
          "answers",
          paste0("scale-factor-", scale_factor_string),
          paste0("tpch-q", sprintf("%02i", query_id), "-sf", scale_factor_string, ".parquet")
        ),
        " in the arrowbench package directory) was not found "
      )
    }

    answer <- arrow::read_parquet(answer_file)
  } else if (source == "duckdb") {
    if (scale_factor != 1) {
      warning("DuckDB answers not at scale_factor 1 aren't easily selectable or available")
      return(NULL)
    }
    ensure_custom_duckdb()
    answer_psv <- query_custom_duckdb(
      paste0(
        "SELECT *, cast(scale_factor AS VARCHAR) FROM tpch_answers() ",
        "WHERE " ,
        "scale_factor = ", scale_factor,
        " AND ",
        "query_nr = ",     query_id,
        ";"
      )
    )
    answer <- utils::read.delim(textConnection(answer_psv$answer), sep = "|")

    # special cases
    # the cntrycode column is a string (c_phone on which it's based is a string
    # and we substring out of it). However because the strings are all digits,
    # csv parsing returns a numeric column, so fix that.
    if (query_id == 22) {
      answer$cntrycode <- as.character(answer$cntrycode)
    }
  }

  answer
}

#' Get a SQL query
#'
#' Produces a function that can be queried against any DBI backend (e.g. DuckDB)
#'
#' The function that is returned takes the following arguments. The first two are
#' suppleid to match the signature of those in tpc_h_queries
#'
#' * `input_func` set to default `NULL`, will have no effect if supplied
#' * `collect_func` set to default `NULL`, will have no effect if supplied
#' * `con` a (DBI) connection to query against
#'
#' @param query_num the query number to fetch the result for
#'
#' @return a function that accepts an argument `con` which will run
#' `DBI::dbGetQuery()` against.
#'
#' @export
#' @keywords internal
get_sql_query_func <- function(query_num) {
  query_sql <- get_sql_tpch_query(query_num)

  # wrap the SQL in a function
  function(input_func = NULL, collect_func = NULL, con) {
    DBI::dbGetQuery(con, query_sql)
  }
}


get_sql_tpch_query <- function(query_num) {
  ensure_custom_duckdb()
  out <- query_custom_duckdb(
    paste0("SELECT query FROM tpch_queries() WHERE query_nr=", query_num, ";")
  )

  # there should be only one row
  stopifnot(nrow(out) == 1)

  # extract the one query found
  out$query[[1]]
}
