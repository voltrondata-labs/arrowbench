#' Benchmark JSON reading
#'
#' @section Parameters:
#' * `source` A JSON file path to read in
#' * `reader` One of `c("arrow", "jsonlite", "ndjson", "jsonify", "RcppSimdJson")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
#' @importFrom R.utils gzip
read_json <- Benchmark(
  "read_json",
  setup = function(source = names(known_sources),
                   reader = c("arrow", "jsonlite", "ndjson", "jsonify", "RcppSimdJson"),
                   compression = c("uncompressed", "gzip"),
                   output = c("arrow_table", "data_frame")) {
    reader <- match.arg(reader)
    compression <- match.arg(compression)
    output <- match.arg(output)

    input_file <- ensure_format(source, "json", compression)

    BenchEnvironment(
      # Map string param name to function
      read_func = get_json_reader(reader),
      input_file = input_file,
      result_dim = get_source_attr(source, "dim"),
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
    result_class <- if (as_data_frame) "data.frame" else c("Table", "ArrowObject")
    correct_format <- inherits(result, result_class)

    stopifnot(
      "The dimensions do not match" = all.equal(dim(result), result_dim),
      "The format isn't correct" = correct_format
    )
    result <- NULL
  },
  valid_params = function(params) {
    drop <- ( params$output == "arrow_table" & params$reader != "arrow" ) |
      ( params$reader == "jsonify" & params$compression == "gzip")
    params[!drop, ]
  },
  packages_used = function(params) {
    if (params$reader == 'RcppSimdJson') {
      c('RcppSimdJson', 'data.table')
    } else {
      params$reader
    }
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
    jsonlite = function(path, ...) {
      con <- get_con(path)
      on.exit(close(con))
      jsonlite::stream_in(con = con)
    },
    ndjson = function(..., as_data_frame) ndjson::stream_in(...),
    jsonify = function(path, ..., as_data_frame) {
      con <- get_con(path)
      on.exit(close(con))
      jsonify::from_ndjson(con, ...)
    },
    RcppSimdJson = function(path, ..., as_data_frame) {
      lines <- readLines(path)
      json <- RcppSimdJson::fparse(lines, single_null = NA, max_simplify_lvl = 3L, ...)
      data.table::rbindlist(json)
    }
  )

  stopifnot("Unsupported reader" = reader %in% names(reader_functions))

  reader_functions[[reader]]
}
