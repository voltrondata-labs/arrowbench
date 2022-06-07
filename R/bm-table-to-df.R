#' Benchmark for reading an Arrow table to a data.frame
#'
#' This flexes conversion to R data structures from Arrow data structures.
#'
#' @section Parameters:
#' * `source` A known-file id to use (it will be read in to a data.frame first)
#'
#' @export
table_to_df <- Benchmark(
  "table_to_df",
  version = "1.0.0",

  setup = function(source = names(known_sources)) {
    source <- ensure_source(source)
    result_dim <- get_source_attr(source, "dim")
    table <- read_source(source, as_data_frame = FALSE)

    transfer_func <- function(table) as.data.frame(table)

    BenchEnvironment(
      transfer_func = transfer_func,
      result_dim = result_dim,
      table = table
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- transfer_func(table)
  },
  after_each = {
    stopifnot("The dimensions do not match" = all.equal(dim(result), result_dim))
    result <- NULL
  },
  valid_params = function(params) params,
  packages_used = function(params) "arrow"
)

