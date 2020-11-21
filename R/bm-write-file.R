#' Benchmark file writing
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of `c("uncompressed", "snappy", "zstd")`
#' * `input` One of `c("arrow_table", "data_frame")`
#'
#' @export
write_file <- Benchmark(
  setup = function(ctx,
                   source,
                   format = c("parquet", "feather", "fst"),
                   compression = c("uncompressed", "snappy", "zstd"),
                   input = c("arrow_table", "data_frame"),
                   ...) {
    source <- ensure_source(source)
    ctx$df <- read_source(source, as_data_frame = match.arg(input) == "data_frame")
    ctx$format <- match.arg(format)
    ctx$compression <- match.arg(compression)

    # Map string param name to functions
    ctx$write_func <- conbench:::get_write_function(format, compression)
  },
  before_each = function(ctx) {
    ctx$result_file <- tempfile()
  },
  run = function(ctx) {
    ctx$write_func(ctx$df, ctx$result_file)
  },
  after_each = function(ctx) {
    stopifnot(file.exists(ctx$result_file))
    unlink(ctx$result_file)
  },
  valid_params = function(params) {
    drop <- params$format != "parquet" & params$compression == "snappy"
    params[!drop,]
  }
)

#' Get a writer
#'
#' @param format
#' @param compression
#'
#' @return
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
