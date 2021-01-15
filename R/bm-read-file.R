#' Benchmark file reading
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `format` One of `c("parquet", "feather", "fst")`
#' * `compression` One of `c("uncompressed", "snappy", "zstd")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_file <- Benchmark("read_file",
  setup = function(ctx,
                   source = names(known_sources),
                   # TODO: break out feather_v1 and feather_v2, feather_v2 only in >= 0.17
                   format = c("parquet", "feather", "fst"),
                   compression = c("uncompressed", "snappy", "zstd"),
                   output = c("arrow_table", "data_frame"),
                   ...) {
    ctx$format <- match.arg(format)
    ctx$compression <- match.arg(compression)
    ctx$output <- match.arg(output)
    # Map string param name to function
    ctx$read_func <- get_read_function(ctx$format)

    source <- ensure_source(source)
    ctx$input_file <- conbench:::file_with_ext(source, paste(ctx$format, ctx$compression, sep = "."))
    ctx$result_dim <- conbench:::get_source_attr(source, "dim")

    if (is.null(ctx$result_dim) || !file.exists(ctx$input_file)) {
      tab <- read_source(source, as_data_frame = ctx$format == "fst")
      if (is.null(ctx$result_dim)) {
        # This means we haven't recorded the dim in known_sources, so compute it
        ctx$result_dim <- dim(tab)
      }
      if (!file.exists(ctx$input_file)) {
        # Create the file in the specified format
        get_write_function(ctx$format, ctx$compression)(tab, ctx$input_file)
        stopifnot(file.exists(ctx$input_file))
      }
    }
  },
  before_each = function(ctx) {
    ctx$result <- NULL
  },
  run = function(ctx) {
    ctx$result <- ctx$read_func(ctx$input_file, as_data_frame = ctx$output == "data_frame")
  },
  after_each = function(ctx) {
    stopifnot(identical(dim(ctx$result), ctx$result_dim))
    ctx$result <- NULL
  },
  valid_params = function(params) {
    drop <- params$compression == "snappy" & params$format != "parquet" |
            params$output == "arrow_table" & params$format == "fst"
    params[!drop,]
  }
)

#' Get a reader
#'
#' @param format
#'
#' @return
#' @export
get_read_function <- function(format) {
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
