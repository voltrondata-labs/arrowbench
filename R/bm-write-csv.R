#' Benchmark CSV writing
#'
#' @section Parameters:
#' * `source` A CSV file path to write to
#' * `writer` One of `c("arrow", "data.table", "vroom", "readr",)`
#' * `input` One of `c("arrow_table", "data_frame")`
#'
#' @export
write_csv <- Benchmark(
  "write_csv",
  setup = function(source = names(known_sources),
                   writer = "arrow",
                   compression = c("uncompressed", "gzip"),
                   input = c("arrow_table", "data_frame")) {
    writer <- match.arg(writer, c("arrow", "data.table", "vroom", "readr", "base"))
    compression <- match.arg(compression, c("uncompressed", "gzip"))
    input <- match.arg(input)

    # source defaults are retrieved from the function definition (all available
    # known_sources) and then read the source in as a data.frame
    source <- ensure_source(source)
    df <- read_source(source, as_data_frame = match.arg(input) == "data_frame")

    ext <- switch(
      compression,
      uncompressed = ".csv",
      gzip = ".csv.gz",
      paste0(".csv.", compression)
    )

    # Map string param name to functions
    BenchEnvironment(
      write_csv_func = get_csv_writer(writer),
      source = source,
      df = df,
      ext = ext
    )
  },
  # delete the results before each iteration
  before_each = {
    result_file <- tempfile(fileext = ext)

  },
  # the benchmark to run
  run = {
    write_csv_func(df, result_file)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    stopifnot(identical(dim(df), dim(arrow::open_dataset(result_file, format = "csv"))))
    stopifnot("Output file does not exist" = file.exists(result_file))
    unlink(result_file)
  },
  valid_params = function(params) {
    ## Only arrow fns will accept an arrow_table
    drop <- ( params$input == "arrow_table" & params$writer != "arrow" )
    params[!drop,]
  },
  packages_used = function(params) {
    params$writer
  }
)


#' Get a CSV writer
#'
#' @param writer the writer to use
#'
#' @return the csv writer
#' @export
get_csv_writer <- function(writer) {
  library(writer, character.only = TRUE, warn.conflicts = FALSE)
  if (writer == "arrow") {
    return(function(...) arrow::write_csv_arrow(...))
  } else if (writer == "readr") {
    return(function(..., as_data_frame) readr::write_csv(...))
  } else if (writer == "data.table") {
    return(function(..., as_data_frame) data.table::fwrite(...))
  } else if (writer == "vroom") {
    return(function(..., as_data_frame) vroom::vroom_write(..., delim = ","))
  } else if (writer == "base") {
    return(function(...) utils::write.csv(..., row.names = FALSE))
  } else {
    stop("Unsupported writer: ", writer, call. = FALSE)
  }
}
