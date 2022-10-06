#' Benchmark file reading
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `file_type` One of `c("parquet", "feather", "fst")`
#' * `compression` One of the values: `r paste(known_compressions, collapse = ", ")`
#' * `output_type` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_file <- Benchmark("file-read",
  setup = function(source = c("fanniemae_2016Q4", "nyctaxi_2010-01"),
                   # TODO: break out feather_v1 and feather_v2, feather_v2 only in >= 0.17
                   file_type = c("parquet", "feather"),
                   compression = c("uncompressed", "snappy", "lz4"),
                   output_type = c("arrow_table", "data_frame")) {
    # file_type defaults to parquet or feather, but can accept fst as well
    file_type <- match.arg(file_type, c("parquet", "feather", "fst"))
    # the output_type defaults are retrieved from the function definition (arrow_table and data_frame)
    output_type <- match.arg(output_type)

    # ensure that we have the right kind of file available
    file_up <- ensure_source(source, file_type, compression)
    input_file <- file_up$path
    # retrieve the dimnesions for run-checking after the benchmark
    result_dim <- file_up$dim

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(
      # get the correct read function for the input file_type
      read_func = get_read_function(file_type),
      input_file = input_file,
      result_dim = result_dim,
      as_data_frame = output_type == "data_frame"
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
    # make sure that the file_type and the compression is compatible
    # and fst doesn't have arrow_table output_type
    drop <- !validate_format(params$file_type, params$compression) |
            params$output_type == "arrow_table" & params$file_type == "fst"
    params[!drop,]
  },
  # packages used when specific file_types are used
  packages_used = function(params) {
    pkg_map <- c(
      "feather" = "arrow",
      "arrow" = "arrow",
      "parquet" = "arrow",
      "fst" = "fst",
      "ndjson" = "arrow"
    )
    pkg_map[params$file_type]
  }
)

#' Get a reader
#'
#' @param file_type what file_type to read
#'
#' @return the read function to use
#' @export
get_read_function <- function(file_type) {
  pkg_map <- c(
    "feather" = "arrow",
    "arrow" = "arrow",
    "parquet" = "arrow",
    "fst" = "fst",
    "ndjson" = "arrow"
  )
  library(pkg_map[[file_type]], character.only = TRUE, warn.conflicts = FALSE)

  if (file_type %in% c("feather", "arrow")) {
    return(function(...) arrow::read_feather(...))
  } else if (file_type == "parquet") {
    return(function(...) arrow::read_parquet(...))
  } else if (file_type == "ndjson") {
    return(function(..., as_data_frame) arrow::read_json_arrow(...))
  } else if (file_type == "fst") {
    return(function(..., as_data_frame) fst::read_fst(...))
  } else {
    stop("Unsupported file_type: ", file_type, call. = FALSE)
  }
}
