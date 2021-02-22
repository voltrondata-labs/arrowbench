#' Benchmark file writing
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of `c("uncompressed", "snappy", "zstd")`
#' * `input` One of `c("arrow_table", "data_frame")`
#'
#' @export
df_to_table <- Benchmark("df_to_table",
  setup = function(source = names(known_sources), ...) {
    source <- ensure_source(source)
    result_dim <- get_source_attr(source, "dim")
    df <- read_source(source, as_data_frame = TRUE)

    transfer_func <- function(df, ...) arrow::Table$create(df, ...)

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

