#' Benchmark CSV reading
#'
#' @section Parameters:
#' * `source` A CSV file path to read in
#' * `reader` One of `c("arrow", "data.table", "vroom", "readr")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output_format` One of `c("arrow_table", "data_frame")`
#'
#' @export
#' @importFrom R.utils gzip
read_csv <- Benchmark(
  "read_csv",
  setup = function(source = known_sources,
                   reader = "arrow",
                   compression = c("uncompressed", "gzip"),
                   output_format = c("arrow_table", "data_frame")) {
    reader <- match.arg(reader, c("arrow", "data.table", "vroom", "readr"))
    compression <- match.arg(compression)
    output_format <- match.arg(output_format)
    # ensure the the file exists
    source <- ensure_source(source, "csv", compression)

    # Map string param name to function
    delim <- source$delim %||% ","
    read_func <- get_csv_reader(reader, delim)
    result_dim <- source$dim

    BenchEnvironment(
      # Map string param name to function
      read_func = get_csv_reader(reader, delim),
      input_file = source$path,
      result_dim = result_dim,
      as_data_frame = output_format == "data_frame",
      delim = delim
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- read_func(input_file, delim = delim, as_data_frame = as_data_frame)
  },
  after_each = {
    correct_format <- FALSE
    if (as_data_frame) {
      correct_format <- inherits(result, "data.frame")
    } else {
      correct_format <- inherits(result, c("Table", "ArrowObject"))
    }

    stopifnot(
      # we have a tolerance of 1 here because vroom reads 1 additional row of
      # all NAs since there are two new lines after the header
      "The dimensions do not match" = all.equal(dim(result), result_dim, tolerance = 1),
      "The format isn't correct" = correct_format
    )
    result <- NULL
  },
  valid_params = function(params) {
    # compression was only supported from arrow 1.0.0 and onward, but `lib_path`
    # may not be set here
    version_check <- (!is.null(params$lib_path) && params$lib_path < "1.0")

    # on macOS data.table doesn't (typically) have multi core support
    # TODO: check if this is actually enabled before running?
    drop <- ( params$output_format == "arrow_table" & params$reader != "arrow" ) |
      ( params$reader == "readr" & params$cpu_count > 1 ) |
      ( params$compression != "uncompressed" & params$reader == "arrow" & version_check )
    params[!drop,]
  },
  packages_used = function(params) {
    params$reader
  }
)

#' Get a CSV reader
#'
#' @param reader the reader to use
#' @param delim the delimiter to use
#'
#' @return the csv reader
#' @export
get_csv_reader <- function(reader, delim) {
  library(reader, character.only = TRUE, warn.conflicts = FALSE)
  # TODO: allow other readers to read non-comma delimed files
  if (reader == "arrow") {
    # TODO: if gzipped and arrow csv reader version doesn't support, unzip?
    return(function(...) arrow::read_delim_arrow(...))
  } else if (reader == "readr") {
    return(function(..., as_data_frame) readr::read_delim(...))
  } else if (reader == "data.table") {
    sep <- force(delim)
    return(function(..., as_data_frame, delim) data.table::fread(..., sep = sep))
  } else if (reader == "vroom") {
    # altrep = FALSE because otherwise you aren't getting the data
    # TODO: maybe we do want to compare, esp. later when we do altrep
    return(function(..., as_data_frame) vroom::vroom(..., altrep = FALSE))
  } else {
    stop("Unsupported reader: ", reader, call. = FALSE)
  }
}
