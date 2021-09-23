#' Benchmark TPC-H queries
#'
#' @section Parameters:
#' * `engine` One of `c("arrow", "duckdb")`
#' * `query_id` integer, 1-22
#' * `format` One of `c("parquet", "feather", "native")`
#' * `scale_factor` Scale factor to use for data generation (e.g. 0.1, 1, 10, 100)
#' * `memory_map` Should memory mapping be used when reading a file in? (only
#'   applicable to arrow, native. `FALSE` will result in the file being explicitly
#'   read into memory before the benchmark)
#'
#' @export
tpc_h <- Benchmark("tpc_h",
  setup = function(engine = "arrow",
                   query_id = c(1, 6),
                   format = c("native", "parquet", "feather"),
                   scale_factor = c(1, 10),
                   memory_map = FALSE) {
    # engine defaults to arrow
    engine <- match.arg(engine, c("arrow", "duckdb"))
    # input format
    format <- match.arg(format, c("parquet", "feather", "native"))
    # query_id defaults to 1 for now
    stopifnot(
      "query_id must be an int" = query_id %% 1 == 0,
      "query_id must 1-22" = query_id >= 1 & query_id <= 22
    )

    library("dplyr")

    # ensure that we have the _base_ tpc-h files (in parquet)
    tpch_files <- ensure_source("tpch", scale_factor = scale_factor)

    # these will be filled in later, when we have file paths available
    input_functions <- list()

    # we use this connection both to populate views/tables and get answer info
    con <- DBI::dbConnect(duckdb::duckdb())

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
            mmap = memory_map
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

    # Get query, and error if it is not implemented
    query <- tpc_h_queries[[query_id]]
    if (is.null(query)) {
      stop("The query ", query_id, " is not yet implemented.", call. = FALSE)
    }

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      input_func = input_functions[[engine]],
      tpch_files = tpch_files,
      query = query,
      engine = engine,
      con = con,
      scale_factor = scale_factor,
      query_id = query_id
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

    # If the scale_factor is < 1, duckdb has the answer
    if (scale_factor %in% c(0.01, 0.1, 1)) {
      # get answers
      answer_psv <- DBI::dbGetQuery(
        con,
        paste0(
          "SELECT answer FROM tpch_answers() WHERE scale_factor = ",
          scale_factor,
          " AND query_nr = ",
          query_id,
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
      # grab scale_factor 1 to check dimensions
      answer_psv <- DBI::dbGetQuery(
        con,
        paste0("SELECT answer FROM tpch_answers() WHERE scale_factor = 1 AND query_nr = ", query_id, ";")
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
      params$memory_map == TRUE & params$engine != "arrow"
    params[!drop,]
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    c(params$engine, "dplyr")
  }
)

#' all queries take an input_func which is a function that will return a dplyr tbl
#' referencing the table needed.
#'
#' @keywords internal
#' @export
tpc_h_queries <- list()

tpc_h_queries[[1]] <- function(input_func) {
  input_func("lineitem") %>%
    select(l_shipdate, l_returnflag, l_linestatus, l_quantity,
           l_extendedprice, l_discount, l_tax) %>%
    # kludge, should be: filter(l_shipdate <= "1998-12-01" - interval x day) %>%
    # where x is between 60 and 120, 90 is the only one that will validate.
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

tpc_h_queries[[2]] <- function(input_func) {
    ps <- input_func("partsupp") %>% select(ps_partkey, ps_suppkey, ps_supplycost)

    p <- input_func("part") %>%
      select(p_partkey, p_type, p_size, p_mfgr) %>%
      filter(p_size == 15, grepl(".*BRASS$", p_type)) %>%
      select(p_partkey, p_mfgr)

    psp <- inner_join(ps, p, by = c("ps_partkey" = "p_partkey"))

    sp <- input_func("supplier") %>%
      select(s_suppkey, s_nationkey, s_acctbal, s_name,
             s_address, s_phone, s_comment)

    psps <- inner_join(psp, sp,
                       by = c("ps_suppkey" = "s_suppkey")) %>%
      select(ps_partkey, ps_supplycost, p_mfgr, s_nationkey,
             s_acctbal, s_name, s_address, s_phone, s_comment)

    nr <- inner_join(input_func("nation"),
                     input_func("region") %>% filter(r_name == "EUROPE"),
                     by = c("n_regionkey" = "r_regionkey")) %>%
      select(n_nationkey, n_name)

    pspsnr <- inner_join(psps, nr,
                         by = c("s_nationkey" = "n_nationkey")) %>%
      select(ps_partkey, ps_supplycost, p_mfgr, n_name, s_acctbal,
             s_name, s_address, s_phone, s_comment)

    aggr <- pspsnr %>%
      group_by(ps_partkey) %>%
      summarise(min_ps_supplycost = min(ps_supplycost))

    sj <- inner_join(pspsnr, aggr,
                     by=c("ps_partkey" = "ps_partkey", "ps_supplycost" = "min_ps_supplycost"))

    res <- sj %>%
      select(s_acctbal, s_name, n_name, ps_partkey, p_mfgr,
             s_address, s_phone, s_comment) %>%
      arrange(desc(s_acctbal), n_name, s_name, ps_partkey) %>%
      head(100)

    collect(res)
}

tpc_h_queries[[3]] <- function(input_func) {
  oc <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_custkey, o_orderdate, o_shippriority) %>%
      # kludge, should be: filter(o_orderdate < "1995-03-15"),
      filter(o_orderdate < as.Date("1995-03-15")),
    input_func("customer") %>%
      select(c_custkey, c_mktsegment) %>%
      filter(c_mktsegment == "BUILDING"),
    by = c("o_custkey" = "c_custkey")
  ) %>%
    select(o_orderkey, o_orderdate, o_shippriority)

  loc <- inner_join(
    input_func("lineitem") %>%
      select(l_orderkey, l_shipdate, l_extendedprice, l_discount) %>%
      filter(l_shipdate > as.Date("1995-03-15")) %>%
      select(l_orderkey, l_extendedprice, l_discount),
    oc, by = c("l_orderkey" = "o_orderkey")
  )

  aggr <- loc %>% mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(l_orderkey, o_orderdate, o_shippriority) %>%
    summarise(revenue = sum(volume)) %>%
    select(l_orderkey, revenue, o_orderdate, o_shippriority) %>%
    arrange(desc(revenue), o_orderdate) %>%
    head(10)

  collect(aggr)
}

tpc_h_queries[[4]] <- function(input_func) {
  l <- input_func("lineitem") %>%
    select(l_orderkey, l_commitdate, l_receiptdate) %>%
    filter(l_commitdate < l_receiptdate) %>%
    select(l_orderkey)

  o <- input_func("orders") %>%
    select(o_orderkey, o_orderdate, o_orderpriority) %>%
    # kludge: filter(o_orderdate >= "1993-07-01", o_orderdate < "1993-07-01" + interval '3' month) %>%
    filter(o_orderdate >= as.Date("1993-07-01"), o_orderdate < as.Date("1993-10-01")) %>%
    select(o_orderkey, o_orderpriority)

  # distinct after join, tested and indeed faster
  lo <- inner_join(l, o, by = c("l_orderkey" = "o_orderkey")) %>%
    distinct() %>%
    select(o_orderpriority)

  aggr <- lo %>%
    group_by(o_orderpriority) %>%
    summarise(order_count = n()) %>%
    arrange(o_orderpriority)

  collect(aggr)
}

tpc_h_queries[[5]] <- function(input_func) {
  nr <- inner_join(
    input_func("nation") %>%
      select(n_nationkey, n_regionkey, n_name),
    input_func("region") %>%
      select(r_regionkey, r_name) %>%
      filter(r_name == "ASIA"),
    by = c("n_regionkey" = "r_regionkey")
  ) %>%
    select(n_nationkey, n_name)

  snr <- inner_join(
    input_func("supplier") %>%
      select(s_suppkey, s_nationkey),
    nr,
    by = c("s_nationkey" = "n_nationkey")
  ) %>%
    select(s_suppkey, s_nationkey, n_name)

  lsnr <- inner_join(
    input_func("lineitem") %>% select(l_suppkey, l_orderkey, l_extendedprice, l_discount),
    snr, by = c("l_suppkey" = "s_suppkey"))

  o <- input_func("orders") %>%
    select(o_orderdate, o_orderkey, o_custkey) %>%
    # kludge: filter(o_orderdate >= "1994-01-01", o_orderdate < "1994-01-01" + interval '1' year) %>%
    filter(o_orderdate >= as.Date("1994-01-01"), o_orderdate < as.Date("1995-01-01")) %>%
    select(o_orderkey, o_custkey)

  oc <- inner_join(o, input_func("customer") %>% select(c_custkey, c_nationkey),
                   by = c("o_custkey" = "c_custkey")) %>%
    select(o_orderkey, c_nationkey)

  lsnroc <- inner_join(lsnr, oc,
                       by = c("l_orderkey" = "o_orderkey", "s_nationkey" = "c_nationkey")) %>%
    select(l_extendedprice, l_discount, n_name)

  aggr <- lsnroc %>%
    mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(n_name) %>%
    summarise(revenue = sum(volume)) %>%
    arrange(desc(revenue))

  collect(aggr)
}

tpc_h_queries[[6]] <- function(input_func) {
  input_func("lineitem") %>%
    select(l_shipdate, l_extendedprice, l_discount, l_quantity) %>%
    # kludge, should be: filter(l_shipdate >= "1994-01-01",
    filter(l_shipdate >= as.Date("1994-01-01"),
           # kludge: should be: l_shipdate < "1994-01-01" + interval '1' year,
           l_shipdate < as.Date("1995-01-01"),
           l_discount >= 0.06 - 0.01,
           l_discount <= 0.06 + 0.01,
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

