#' Benchmark JSON reading
#'
#' @section Parameters:
#' * `source` A JSON file path to read in
#' * `reader` One of `c("arrow", "jsonlite", "ndjson", "RcppSimdJson")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output_format` One of `c("arrow_table", "data_frame")`
#' * `rbinder` Method for simplifying to dataframe. Not relevant for {arrow} and {ndjson}.
#'
#' @export
#' @importFrom R.utils gzip
read_json <- Benchmark(
  "read_json",
  setup = function(source = known_sources,
                   reader = c("arrow", "jsonlite", "ndjson", "RcppSimdJson"),
                   compression = c("uncompressed", "gzip"),
                   output_format = c("arrow_table", "data_frame"),
                   rbinder = c("package", "dplyr", "data.table", "base")) {
    reader <- match.arg(reader)
    compression <- match.arg(compression)
    output_format <- match.arg(output_format)
    rbinder <- match.arg(rbinder)

    rbind_func <- switch(
      rbinder,
      package = identity,
      dplyr = dplyr::bind_rows,
      data.table = data.table::rbindlist,
      # Warning! Base solution will break if ndjson has implicit nulls or varying field order
      base = function(...) do.call(rbind.data.frame, ...)
    )

    source <- ensure_source(source, "ndjson", compression)

    BenchEnvironment(
      # Map string param name to function
      read_func = get_json_reader(reader),
      input_file = source$path,
      result_dim = source$dim,
      as_data_frame = output_format == "data_frame",
      rbinder = rbinder,
      rbind_func = rbind_func
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- read_func(input_file, rbinder = rbinder, as_data_frame = as_data_frame)
    result <- rbind_func(result)
  },
  after_each = {
    result_class <- if (as_data_frame) "data.frame" else c("Table", "ArrowObject")
    correct_format <- inherits(result, result_class)

    # TODO revert to error once null types are removed from sources
    if (ncol(result) != result_dim[2]) {
      warning("The number of columns do not match; null types dropped or data missing", call. = FALSE)
    }
    stopifnot(
      # "The dimensions do not match" = all.equal(dim(result), result_dim),
      "The number of rows does not match" = nrow(result) == result_dim[1],
      "The format isn't correct" = correct_format
    )
    result <- NULL
  },
  valid_params = function(params) {
    drop <- ( params$output_format == "arrow_table" & params$reader != "arrow" ) |
      ( params$reader == "jsonify" & params$compression == "gzip" ) |
      ( params$rbinder != "package" & params$reader %in% c("arrow", "ndjson") ) |
      ( params$rbinder == "package" & params$reader == "RcppSimdJson")
    params[!drop, ]
  },
  packages_used = function(params) {
    packages <- params$reader

    for (rbinder in params$rbinder)
      packages <- c(packages, switch(
        rbinder,
        default = NULL,
        dplyr = "dplyr",
        data.table = "data.table",
        base = NULL
      ))

    unique(packages)
  }
)

#' Get a JSON reader
#'
#' @param reader string of the reader package to use
#'
#' @return the JSON function
#'
#' @export
get_json_reader <- function(reader) {
  get_con <- function(path) {
    if (endsWith(path, ".gz")) {
      con <- gzfile(path, open = "rb")
    } else {
      con <- file(path, open = "r")
    }
    con
  }

  reader_functions <- list(
    arrow = arrow::read_json_arrow,
    jsonlite = function(path, rbinder, ...) {
      con <- get_con(path)
      on.exit(close(con))
      out <- jsonlite::stream_in(
        con = con,
        simplifyVector = rbinder == "package",
        simplifyMatrix = FALSE,
        simplifyDataFrame = rbinder == "package",
        verbose = FALSE
      )
      # no option to use NA instead of NULL, and `do.call(rbind.data.frame, ...)` can't handle the latter
      if (rbinder == "base") {
        out <- lapply(out, function(x) lapply(x, function(y) if (is.null(y)) NA else y))
      }
      out
    },
    ndjson = function(..., rbinder, as_data_frame) ndjson::stream_in(...),
    # NOTE: Removed jsonify from default options because it's unstable
    jsonify = function(path, rbinder, ..., as_data_frame) {
      con <- get_con(path)
      on.exit(close(con))
      jsonify::from_ndjson(con, simplify = rbinder == "package", ...)
    },
    RcppSimdJson = function(path, ..., rbinder, as_data_frame) {
      lines <- readLines(path)
      json <- RcppSimdJson::fparse(lines, single_null = NA, max_simplify_lvl = 3L, ...)
    }
  )

  stopifnot("Unsupported reader" = reader %in% names(reader_functions))

  reader_functions[[reader]]
}
