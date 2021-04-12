#' Benchmark file writing
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of the values: `r paste(known_compressions, collapse = ", ")`
#' * `input` One of `c("arrow_table", "data_frame")`
#'
#' @export
write_file <- Benchmark("write_file",
  setup = function(source = names(known_sources),
                   format = c("parquet", "feather"),
                   compression = c("uncompressed", "snappy", "zstd", "lz4"),
                   input = c("arrow_table", "data_frame")) {
    # source defaults are retrieved from the function definition (all available
    # known_sources) and then read the source in as a data.frame
    source <- ensure_source(source)
    df <- read_source(source, as_data_frame = match.arg(input) == "data_frame")
    # format defaults to parquet or feather, but can accept fst as well
    format <- match.arg(format, c("parquet", "feather", "fst"))

    # Map string param name to functions
    write_func <- get_write_function(format, compression)

    # put the necessary variables into a BenchmarkEnvironment to be used when
    # the benchmark is running.
    BenchEnvironment(
      write_func = write_func,
      format = format,
      source = source,
      df = df
    )
  },
  # delete the results before each iteration
  before_each = {
    result_file <- tempfile()
  },
  # the benchmark to run
  run = {
    write_func(df, result_file)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    stopifnot(file.exists(result_file))
    unlink(result_file)
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    # make sure that the format and the compression is compatible
    # and fst doesn't have arrow_table input
    drop <- !validate_format(params$format, params$compression) |
      params$format == "fst" & params$input == "arrow_table"
    params[!drop,]
  },
  # packages used when specific formats are used
  packages_used = function(params) {
    pkg_map <- c(
      "feather" = "arrow",
      "parquet" = "arrow",
      "fst" = "fst"
    )
    unique(pkg_map[params$format])
  }
)

#' Get a writer
#'
#' @param format format to write
#' @param compression compression to use
#'
#' @return the write function to use
#' @export
get_write_function <- function(format, compression) {
  force(compression)
  if (format == "feather") {
    return(function(...) arrow::write_feather(..., compression = compression))
  } else if (format == "parquet") {
    return(function(...) arrow::write_parquet(..., compression = compression))
  } else if (format == "fst") {
    # fst is always zstd, just a question of what level of compression
    level <- ifelse(compression == "uncompressed", 0, 50)
    return(function(...) fst::write_fst(..., compress = level))
  } else {
    stop("Unsupported format: ", format, call. = FALSE)
  }
}
