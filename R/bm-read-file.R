#' Benchmark file reading
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of the values: `r paste(known_compressions, collapse = ", ")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_file <- Benchmark("read_file",
  setup = function(source = c("fanniemae_2016Q4", "nyctaxi_2010-01"),
                   # TODO: break out feather_v1 and feather_v2, feather_v2 only in >= 0.17
                   format = c("parquet", "feather"),
                   compression = c("uncompressed", "snappy", "lz4"),
                   output = c("arrow_table", "data_frame")) {
    # format defaults to parquet or feather, but can accept fst as well
    format <- match.arg(format, c("parquet", "feather", "fst"))
    # the output defaults are retrieved from the function definition (arrow_table and data_frame)
    output <- match.arg(output)

    # ensure that we have the right kind of file available
    input_file <- ensure_format(source, format, compression)
    # retrieve the dimnesions for run-checking after the benchmark
    result_dim <- get_source_attr(source, "dim")

    # put the necesary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input format
      read_func = get_read_function(format),
      input_file = input_file,
      result_dim = result_dim,
      as_data_frame = output == "data_frame"
    )
  },
  # delete the results before each iteration
  before_each = {
    result <- NULL
  },
  # the benchmark to run
  run = {
    result <- read_func(input_file, as_data_frame = as_data_frame)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    stopifnot(identical(dim(result), result_dim))
    result <- NULL
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    # make sure that the format and the compression is compatible
    # and fst doesn't have arrow_table output
    drop <- !validate_format(params$format, params$compression) |
            params$output == "arrow_table" & params$format == "fst"
    params[!drop,]
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    pkg_map <- c(
      "feather" = "arrow",
      "parquet" = "arrow",
      "fst" = "fst"
    )
    pkg_map[params$format]
  }
)

#' Get a reader
#'
#' @param format what format to read
#'
#' @return the read function to use
#' @export
get_read_function <- function(format) {
  pkg_map <- c(
    "feather" = "arrow",
    "parquet" = "arrow",
    "fst" = "fst"
  )
  library(pkg_map[[format]], character.only = TRUE, warn.conflicts = FALSE)

  if (format == "feather") {
    return(function(...) arrow::read_feather(...))
  } else if (format == "parquet") {
    return(function(...) arrow::read_parquet(...))
  } else if (format == "fst") {
    return(function(..., as_data_frame) fst::read_fst(...))
  } else {
    stop("Unsupported format: ", format, call. = FALSE)
  }
}
