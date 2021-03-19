#' Benchmark file reading
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of `c("uncompressed", "snappy", "zstd", "lz4")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_file <- Benchmark("read_file",
  setup = function(source = names(known_sources),
                   # TODO: break out feather_v1 and feather_v2, feather_v2 only in >= 0.17
                   format = c("parquet", "feather", "fst"),
                   compression = c("uncompressed", "snappy", "zstd", "lz4"),
                   output = c("arrow_table", "data_frame")) {
    format <- match.arg(format)
    output <- match.arg(output)

    # ensure that we have the right kind of file available
    input_file <- ensure_format(source, format, compression)
    result_dim <- get_source_attr(source, "dim")

    BenchEnvironment(
      # Map string param name to function
      read_func = get_read_function(format),
      input_file = input_file,
      result_dim = result_dim,
      as_data_frame = output == "data_frame"
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- read_func(input_file, as_data_frame = as_data_frame)
  },
  after_each = {
    stopifnot(identical(dim(result), result_dim))
    result <- NULL
  },
  valid_params = function(params) {
    drop <- params$compression == "snappy" & params$format != "parquet" |
            params$output == "arrow_table" & params$format == "fst"
    params[!drop,]
  },
  packages_used = function(params) {
    pkg_map <- c(
      "feather" = "arrow",
      "parquet" = "arrow",
      "fst" = "fst"
    )
    unique(pkg_map[params$format])
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
  library(pkg_map[[format]], character.only = TRUE)

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
