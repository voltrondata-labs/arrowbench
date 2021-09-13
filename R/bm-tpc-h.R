#' Benchmark TPC-H queries
#'
#' @section Parameters:
#' * `engine` One of `c("arrow", "duckdb", "dplyr)`
#' * `query_num` integer, 1-22
#' * `scale` Scale factor to use for data generation
#'
#' @export
tpc_h <- Benchmark("tpc_h",
  setup = function(engine = "arrow",
                   query_num = 1,
                   scale = 10) {
    # engine defaults to arrow
    engine <- match.arg(engine, c("arrow", "duckdb", "dplyr"))
    # query_num defaults to 1 for now
    stopifnot(
      "query_num must be an int" = query_num %% 1 == 0,
      "query_num must 1-22" = query_num >= 1 & query_num <= 22
    )

    library("dplyr")

    # ensure that we have the right kind of files available
    tpch_filenames <- ensure_source("tpch", scale = scale)

    input_functions <- list(
      arrow = function(name) {
        file <- tpch_filenames[[name]]
        return(arrow::read_parquet(file, as_data_frame = FALSE))
      },
      duckdb = function(name) {
        return(tbl(con, name))
      }
    )

    # We use this connection both to populate views/tables and get answer info
    con <- DBI::dbConnect(duckdb::duckdb())

    if (engine == "duckdb") {
      con <- DBI::dbConnect(duckdb::duckdb())
      # set parallelism for duckdb
      DBI::dbExecute(con, paste0("PRAGMA threads=", getOption("Ncpus")))

      for (name in tpch_tables) {
        file <- path.expand(tpch_filenames[[name]])

        # have to create a VIEW in order to reference it by name
        # This view is the most accurate comparison to Arrow, however it will
        # penalize duckdb since AFAICT `parquet_scan` is not parallelized and
        # ends up being the bottleneck
        DBI::dbExecute(
          con,
          paste0("CREATE VIEW ", name, " AS SELECT * FROM parquet_scan('", file, "');")
        )
      }
    }

    # get answers
    answer_psv <- DBI::dbGetQuery(
      con,
      paste0("SELECT answer FROM tpch_answers() WHERE scale_factor = 1 AND query_nr = ", query_num, ";")
    )
    answer <- read.delim(textConnection(answer_psv$answer), sep = "|")

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      input_func = input_functions[[engine]],
      tpch_filenames = tpch_filenames,
      query = tpc_h_queries[[as.character(query_num)]],
      engine = engine,
      con = con,
      answer = answer
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
    if (!isTRUE(all.equal(result, answer, check.attributes = FALSE, tolerance = 0.001))) {
      warning("Expected:\n\n", paste0(capture.output(print(answer)), collapse = "\n"))
      warning("\n\nGot:\n\n", paste0(capture.output(print(result, width = Inf)), collapse = "\n"))
      stop("The answer does not match")
    }
    result <- NULL
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    params
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    c(params$engine, "dplyr")
  }
)

# all queries take an input_func which is a function that will return a dplyr tbl
# referencing the table needed.
#' @export
tpc_h_queries <- list(
  "1" = function(input_func) {
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
  },
  "6" = function(input_func) {
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
)

