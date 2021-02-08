#' Benchmark file writing
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of `c("uncompressed", "snappy", "zstd")`
#' * `input` One of `c("arrow_table", "data_frame")`
#'
#' @export
write_file <- Benchmark("write_file",
  setup = function(source = names(known_sources),
                   format = c("parquet", "feather", "fst"),
                   compression = c("uncompressed", "snappy", "zstd"),
                   input = c("arrow_table", "data_frame"),
                   ...) {
    source <- ensure_source(source)
    df <- read_source(source, as_data_frame = match.arg(input) == "data_frame")
    format <- match.arg(format)
    compression <- match.arg(compression)

    # Map string param name to functions
    write_func <- get_write_function(format, compression)

    BenchEnvironment(
      write_func = write_func,
      format = format,
      source = source,
      df = df
    )
  },
  before_each = {
    result_file <- tempfile()
  },
  run = {
    write_func(df, result_file)
  },
  after_each = {
    stopifnot(file.exists(result_file))
    unlink(result_file)
  },
  valid_params = function(params) {
    drop <- params$format != "parquet" & params$compression == "snappy"
    params[!drop,]
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
