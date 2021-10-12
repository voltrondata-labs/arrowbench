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

      # TODO: different tolerances for different kinds of columns?
      # > For ratios, results r must be within 1% of the query validation output
      # data v when rounded to the nearest 1/100th. That is, 0.99*v<=round(r,2)<=1.01*v.
      # > For results from AVG aggregates, the resulting values r must be within 1%
      # of the query validation output data when rounded to the nearest 1/100th
      # > For results from SUM aggregates, the resulting values must be within
      # $100 of the query validation output data.
      all_equal_out <- all.equal(result, answer, check.attributes = FALSE, tolerance = 0.01)

      # turn chars into dates in the answer (in DuckDB, they are all chars not dates)
      # TODO: send duckdb a PR to change that?
      char_to_date <- purrr::map_lgl(all_equal_out, ~grepl("target is Date, current is character", .x))
      cols <- sub("Component “(.*)”.*", "\\1", all_equal_out[char_to_date])
      for (col in cols) {
        answer[, col] <- as.Date(answer[, col])
      }

      all_equal_out <- all.equal(result, answer, check.attributes = FALSE, tolerance = 0.01)
      if (!isTRUE(all_equal_out)) {
        warning("\n", all_equal_out, "\n")
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

    res %>%
      collect()
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

  aggr %>%
    collect()
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
           # Should be the following, but https://issues.apache.org/jira/browse/ARROW-14125
           # Need to round because 0.06 - 0.01 != 0.05
           l_discount >= round(0.06 - 0.01, digits = 2),
           l_discount <= round(0.06 + 0.01, digits = 2),
           # l_discount >= 0.05,
           # l_discount <= 0.07,
           l_quantity < 24) %>%
    select(l_extendedprice, l_discount) %>%
    summarise(revenue = sum(l_extendedprice * l_discount)) %>%
    collect()
}

tpc_h_queries[[7]] <- function(input_func) {
  sn <- inner_join(
    input_func("supplier") %>%
      select(s_nationkey, s_suppkey),
    input_func("nation") %>%
      select(n1_nationkey = n_nationkey, n1_name = n_name) %>%
      filter(n1_name %in% c("FRANCE", "GERMANY")),
    by = c("s_nationkey" = "n1_nationkey")) %>%
    select(s_suppkey, n1_name)

  cn <- inner_join(
    input_func("customer") %>%
      select(c_custkey, c_nationkey),
    input_func("nation") %>%
      select(n2_nationkey = n_nationkey, n2_name = n_name) %>%
      filter(n2_name %in% c("FRANCE", "GERMANY")),
    by = c("c_nationkey" = "n2_nationkey")) %>%
    select(c_custkey, n2_name)

  cno <- inner_join(
    input_func("orders") %>%
      select(o_custkey, o_orderkey),
    cn, by = c("o_custkey" = "c_custkey")) %>%
    select(o_orderkey, n2_name)

  cnol <- inner_join(
    input_func("lineitem") %>%
      select(l_orderkey, l_suppkey, l_shipdate, l_extendedprice, l_discount) %>%
      # kludge, should be: filter(l_shipdate >= "1995-01-01", l_shipdate <= "1996-12-31"),
      filter(l_shipdate >= as.Date("1995-01-01"), l_shipdate <= as.Date("1996-12-31")),
    cno,
    by = c("l_orderkey" = "o_orderkey")) %>%
    select(l_suppkey, l_shipdate, l_extendedprice, l_discount, n2_name)

  all <- inner_join(cnol, sn, by = c("l_suppkey" = "s_suppkey"))

  aggr <- all %>%
    filter((n1_name == "FRANCE" & n2_name == "GERMANY") |
             (n1_name == "GERMANY" & n2_name == "FRANCE")) %>%
    mutate(
      supp_nation = n1_name,
      cust_nation = n2_name,
      # kludge (?) l_year = as.integer(strftime(l_shipdate, "%Y")),
      l_year = year(l_shipdate),
      volume = l_extendedprice * (1 - l_discount)) %>%
    select(supp_nation, cust_nation, l_year, volume) %>%
    group_by(supp_nation, cust_nation, l_year) %>%
    summarise(revenue = sum(volume)) %>%
    arrange(supp_nation, cust_nation, l_year)

  aggr %>%
    collect()
}

tpc_h_queries[[8]] <- function(input_func) {
  # kludge, swapped the table order around because of ARROW-14184
  # nr <- inner_join(
  #   input_func("nation") %>%
  #     select(n1_nationkey = n_nationkey, n1_regionkey = n_regionkey),
  #   input_func("region") %>%
  #     select(r_regionkey, r_name) %>%
  #     filter(r_name == "AMERICA") %>%
  #     select(r_regionkey),
  #   by = c("n1_regionkey" = "r_regionkey")) %>%
  #   select(n1_nationkey)
  nr <- inner_join(
    input_func("region") %>%
      select(r_regionkey, r_name) %>%
      filter(r_name == "AMERICA") %>%
      select(r_regionkey),
    input_func("nation") %>%
      select(n1_nationkey = n_nationkey, n1_regionkey = n_regionkey),
    by = c("r_regionkey" = "n1_regionkey")) %>%
    select(n1_nationkey)

  cnr <- inner_join(
    input_func("customer") %>%
      select(c_custkey, c_nationkey),
    nr, by = c("c_nationkey" = "n1_nationkey")) %>%
    select(c_custkey)

  ocnr <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_custkey, o_orderdate) %>%
      # bludge, should be: filter(o_orderdate >= "1995-01-01", o_orderdate <= "1996-12-31"),
      filter(o_orderdate >= as.Date("1995-01-01"), o_orderdate <= as.Date("1996-12-31")),
    cnr, by = c("o_custkey" = "c_custkey")) %>%
    select(o_orderkey, o_orderdate)

  locnr <- inner_join(
    input_func("lineitem") %>%
      select(l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount),
    ocnr, by=c("l_orderkey" = "o_orderkey")) %>%
    select(l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderdate)

  locnrp <- inner_join(locnr,
                       input_func("part") %>%
                         select(p_partkey, p_type) %>%
                         filter(p_type == "ECONOMY ANODIZED STEEL") %>%
                         select(p_partkey),
                       by = c("l_partkey" = "p_partkey")) %>%
    select(l_suppkey, l_extendedprice, l_discount, o_orderdate)

  locnrps <- inner_join(locnrp,
                        input_func("supplier") %>%
                          select(s_suppkey, s_nationkey),
                        by = c("l_suppkey" = "s_suppkey")) %>%
    select(l_extendedprice, l_discount, o_orderdate, s_nationkey)

  all <- inner_join(locnrps,
                    input_func("nation") %>%
                      select(n2_nationkey = n_nationkey, n2_name = n_name),
                    by = c("s_nationkey" = "n2_nationkey")) %>%
    select(l_extendedprice, l_discount, o_orderdate, n2_name)

  aggr <- all %>%
    mutate(
      # kludge(?), o_year = as.integer(strftime(o_orderdate, "%Y")),
      o_year = year(o_orderdate),
      volume = l_extendedprice * (1 - l_discount),
      nation = n2_name) %>%
    select(o_year, volume, nation) %>%
    group_by(o_year) %>%
    summarise(mkt_share = sum(ifelse(nation == "BRAZIL", volume, 0)) / sum(volume)) %>%
    arrange(o_year)

  aggr %>%
    collect()
}

tpc_h_queries[[9]] <- function(input_func) {
  p <- input_func("part") %>%
    select(p_name, p_partkey) %>%
    filter(grepl(".*green.*", p_name)) %>%
    select(p_partkey)

  psp <- inner_join(
    input_func("partsupp") %>%
      select(ps_suppkey, ps_partkey, ps_supplycost),
    p, by = c("ps_partkey" = "p_partkey"))

  sn <- inner_join(
    input_func("supplier") %>%
      select(s_suppkey, s_nationkey),
    input_func("nation") %>%
      select(n_nationkey, n_name),
    by = c("s_nationkey" = "n_nationkey")) %>%
    select(s_suppkey, n_name)

  pspsn <- inner_join(psp, sn, by = c("ps_suppkey" = "s_suppkey"))

  lpspsn <- inner_join(
    input_func("lineitem") %>%
      select(l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, l_quantity),
    pspsn,
    by = c("l_suppkey" = "ps_suppkey", "l_partkey" = "ps_partkey")) %>%
    select(l_orderkey, l_extendedprice, l_discount, l_quantity, ps_supplycost, n_name)

  all <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_orderdate),
    lpspsn,
    by = c("o_orderkey"= "l_orderkey" )) %>%
    select(l_extendedprice, l_discount, l_quantity, ps_supplycost, n_name, o_orderdate)

  aggr <- all %>%
    # kludge, should not need to compute here, but if we don't the answers we
    # get are wrong (though they *do* complete).
    mutate(
      nation = n_name,
      # kludge, o_year = as.integer(format(o_orderdate, "%Y")),
      # also ARROW-14200
      o_year = year(o_orderdate),
      amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) %>%
    select(nation, o_year, amount) %>%
    group_by(nation, o_year) %>%
    summarise(sum_profit = sum(amount)) %>%
    arrange(nation, desc(o_year))

  aggr %>%
    collect()
}

tpc_h_queries[[10]] <- function(input_func) {
  l <- input_func("lineitem") %>%
    select(l_orderkey, l_returnflag, l_extendedprice, l_discount) %>%
    filter(l_returnflag == "R") %>%
    select(l_orderkey, l_extendedprice, l_discount)

  o <- input_func("orders") %>%
    select(o_orderkey, o_custkey, o_orderdate) %>%
    # kludge, filter(o_orderdate >= "1993-10-01", o_orderdate < "1994-01-01") %>%
    filter(o_orderdate >= as.Date("1993-10-01"), o_orderdate < as.Date("1994-01-01")) %>%
    select(o_orderkey, o_custkey)

  lo <- inner_join(l, o,
                   by = c("l_orderkey" = "o_orderkey")) %>%
    select(l_extendedprice, l_discount, o_custkey)
  # first aggregate, then join with customer/nation,
  # otherwise we need to aggr over lots of cols

  lo_aggr <- lo %>% mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(o_custkey) %>%
    summarise(revenue = sum(volume))

  c <- input_func("customer") %>%
    select(c_custkey, c_nationkey, c_name, c_acctbal, c_phone, c_address, c_comment)

  loc <- inner_join(lo_aggr, c, by = c("o_custkey" = "c_custkey"))

  locn <- inner_join(loc, input_func("nation") %>% select(n_nationkey, n_name),
                     by = c("c_nationkey" = "n_nationkey"))

  res <- locn %>%
    select(o_custkey, c_name, revenue, c_acctbal, n_name,
           c_address, c_phone, c_comment) %>%
    arrange(desc(revenue)) %>%
    head(20)

  res %>%
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

