#' Benchmark TPC-H queries
#'
#' @section Parameters:
#' * `engine` One of `c("arrow", "duckdb")`
#' * `query_num` integer, 1-22
#' * `format` One of `c("parquet", "feather", "native")`
#' * `scale` Scale factor to use for data generation (e.g. 0.1, 1, 10, 100)
#' * `mem_map` Should memory mapping be used when reading a file in? (only
#'   applicable to arrow, native. `FALSE` will result in the file being explicitly
#'   read into memory before the benchmark)
#'
#' @export
tpc_h <- Benchmark("tpc_h",
  setup = function(engine = "arrow",
                   query_num = c(1, 6),
                   format = c("native", "parquet", "feather"),
                   scale = c(1, 10),
                   mem_map = FALSE) {
    # engine defaults to arrow
    engine <- match.arg(engine, c("arrow", "duckdb"))
    # input format
    format <- match.arg(format, c("parquet", "feather", "native"))
    # query_num defaults to 1 for now
    stopifnot(
      "query_num must be an int" = query_num %% 1 == 0,
      "query_num must 1-22" = query_num >= 1 & query_num <= 22
    )

    library("dplyr")

    # ensure that we have the _base_ tpc-h files (in parquet)
    tpch_files <- ensure_source("tpch", scale = scale)

    # these will be filled in later, when we have file paths available
    input_functions <- list()

    # we use this connection both to populate views/tables and get answer info
    con <- DBI::dbConnect(duckdb::duckdb())

    # find only the tables that are needed to process
    tpch_tables_needed <- tables_refed(tpc_h_queries[[query_num]])

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
        format = format_to_convert
      )

      # specify readers for each format
      if (format == "parquet") {
        input_functions[["arrow"]] <- function(name) {
          file <- tpch_files[[name]]
          return(arrow::open_dataset(file, format = "parquet"))
        }
      } else if (format == "feather") {
        input_functions[["arrow"]] <- function(name) {
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
            mmap = mem_map
          )
        }
        input_functions[["arrow"]] <- function(name) {
          return(tab[[name]])
        }
      }
    } else if (engine == "duckdb") {
      # set parallelism for duckdb
      DBI::dbExecute(con, paste0("PRAGMA threads=", getOption("Ncpus")))

      input_functions[["duckdb"]] <- function(name) {
        return(tbl(con, name))
      }

      for (name in tpch_tables_needed) {
        file <- path.expand(tpch_files[[name]])

        # have to create a VIEW in order to reference it by name
        # This view is the most accurate comparison to Arrow, however it will
        # penalize duckdb since AFAICT `parquet_scan` is not parallelized and
        # ends up being the bottleneck
        if (format == "parquet") {
          sql_query <- paste0("CREATE VIEW ", name, " AS SELECT * FROM parquet_scan('", file, "');")
        } else if (format == "native") {
          sql_query <- paste0("CREATE TABLE ", name, " AS SELECT * FROM parquet_scan('", file, "');")
        }

        DBI::dbExecute(con, sql_query)
      }
    }

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      input_func = input_functions[[engine]],
      tpch_files = tpch_files,
      query = tpc_h_queries[[query_num]],
      engine = engine,
      con = con,
      scale = scale,
      query_num = query_num
    )
  },
  # delete the results before each iteration
  before_each = {
    result <- NULL
  },
  # the benchmark to run
  run = {
    result <- query(input_func)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    # The tpch_answers() function only exists when the tpch extension has been
    # built
    ensure_custom_duckdb()

    # If the scale is < 1, duckdb has the answer
    if (scale %in% c(0.01, 0.1, 1)) {
      # get answers
      answer_psv <- DBI::dbGetQuery(
        con,
        paste0(
          "SELECT answer FROM tpch_answers() WHERE scale_factor = ",
          scale,
          " AND query_nr = ",
          query_num,
          ";"
        )
      )
      answer <- read.delim(textConnection(answer_psv$answer), sep = "|")

      if (!isTRUE(all.equal(result, answer, check.attributes = FALSE, tolerance = 0.001))) {
        warning("\nExpected:\n", paste0(capture.output(print(answer)), collapse = "\n"))
        warning("\n\nGot:\n", paste0(capture.output(print(result, width = Inf)), collapse = "\n"))
        stop("The answer does not match")
      }
    } else {
      # grab scale factor 1 to check dimensions
      answer_psv <- DBI::dbGetQuery(
        con,
        paste0("SELECT answer FROM tpch_answers() WHERE scale_factor = 1 AND query_nr = ", query_num, ";")
      )
      answer <- read.delim(textConnection(answer_psv$answer), sep = "|")
    }

    # TODO: other generic validations for SF < 1?
    stopifnot(
      "Dims do not match the answer" = identical(dim(result), dim(answer))
    )

    result <- NULL
  },
  teardown = {
    DBI::dbDisconnect(con, shutdown = TRUE)
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    drop <- ( params$engine == "duckdb" & params$format == "feather" ) |
      ( params$engine == "duckdb" & params$format == "feather-dataset" ) |
      ( params$engine == "duckdb" & params$format == "parquet-dataset" ) |
      params$mem_map == TRUE & params$engine != "arrow"
    params[!drop,]
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    c(params$engine, "dplyr")
  }
)

# all queries take an input_func which is a function that will return a dplyr tbl
# referencing the table needed.
#' @export
tpc_h_queries <- list()

tpc_h_queries[[1]] <- function(input_func) {
  input_func("lineitem") %>%
    select(l_shipdate, l_returnflag, l_linestatus, l_quantity,
           l_extendedprice, l_discount, l_tax) %>%
    filter(l_shipdate <= as.Date("1998-09-02")) %>%
    select(l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax) %>%
    group_by(l_returnflag, l_linestatus) %>%
    summarise(
      sum_qty = sum(l_quantity),
      sum_base_price = sum(l_extendedprice),
      sum_disc_price = sum(l_extendedprice * (1 - l_discount)),
      sum_charge = sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
      avg_qty = mean(l_quantity),
      avg_price = mean(l_extendedprice),
      avg_disc = mean(l_discount),
      count_order = n()
    ) %>%
    arrange(l_returnflag, l_linestatus) %>%
    collect()
}

tpc_h_queries[[6]] <- function(input_func) {
  input_func("lineitem") %>%
    select(l_shipdate, l_extendedprice, l_discount, l_quantity) %>%
    filter(l_shipdate >= as.Date("1994-01-01"),
           l_shipdate < as.Date("1995-01-01"),
           # discounts are saved as decimals
           l_discount >= 0.05,
           l_discount <= 0.07,
           l_quantity < 24) %>%
    select(l_extendedprice, l_discount) %>%
    summarise(revenue = sum(l_extendedprice * l_discount)) %>%
    collect()
}

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

