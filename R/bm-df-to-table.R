#' Benchmark for reading a data.frame into an Arrow table
#'
#' This flexes that conversion from R data structures to Arrow data structures.
#'
#' @section Parameters:
#' * `source` A known-file id to use (it will be read in to a data.frame first)
#'
#' @export
df_to_table <- Benchmark("dataframe-to-table",
  setup = function(source = c("chi_traffic_2020_Q1",
                              "type_strings",
                              "type_dict",
                              "type_integers",
                              "type_floats",
                              "type_nested")) {
    source <- ensure_source(source)
    result_dim <- get_source_attr(source, "dim")
    # Make sure that we're not (accidentally) creating altrep vectors which will
    # make the benchmark measure both arrow->R and then also R->arrow when we
    # really want to just measure R->arrow.
    df <- read_source(source, as_data_frame = TRUE)

    transfer_func <- function(df) arrow::Table$create(df)

    BenchEnvironment(
      transfer_func = transfer_func,
      result_dim = result_dim,
      df = df
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- transfer_func(df)
  },
  after_each = {
    stopifnot("The dimensions do not match" = all.equal(dim(result), result_dim))
    result <- NULL
  },
  valid_params = function(params) params,
  packages_used = function(params) "arrow"
)

