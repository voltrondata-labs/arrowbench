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
      "query_num must 1-19" = query_num >= 1 & query_num <= 22
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

    # For Arrow connection is NULL, but for duckdb we need to pass it around.
    con <- NULL
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

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      input_func = input_functions[[engine]],
      tpch_filenames = tpch_filenames,
      query = tpc_h_queries[[query_num]],
      engine = engine,
      con = con
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
    # TODO: check the query answer is correct
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
  one = function(input_func) {
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
  six = function(input_func) {
      input_func("lineitem") %>%
        select(l_shipdate, l_extendedprice, l_discount, l_quantity) %>%
        filter(l_shipdate >= "1994-01-01",
               l_shipdate < "1995-01-01",
               l_discount >= 0.05,
               l_discount <= 0.07,
               l_quantity < 24) %>%
        select(l_extendedprice, l_discount) %>%
        summarise(revenue = sum(l_extendedprice * l_discount)) %>%
        collect()
  }
)

