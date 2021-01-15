#' Benchmark CSV reading
#'
#' @section Parameters:
#' * `source` A CSV file path to read in
#' * `reader` One of `c("arrow", "data.table", "vroom", "readr")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_file <- Benchmark("read_file",
   setup = function(ctx,
                    source = names(known_sources),
                    reader = c("arrow", "data.table", "vroom", "readr"),
                    compression = c("uncompressed", "gzip"),
                    output = c("arrow_table", "data_frame"),
                    ...) {
     ctx$reader <- match.arg(reader)
     ctx$compression <- match.arg(compression)
     ctx$output <- match.arg(output)
     # Map string param name to function
     ctx$read_func <- get_csv_reader(ctx$reader)

     source <- ensure_source(source)
     ctx$result_dim <- conbench:::get_source_attr(source, "dim")

     if(ctx$compression == "gzip") {
       # Check file extension, compress if not found
     } else {
       # Check file extension, decompress if not found
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
     drop <- params$output == "arrow_table" & params$reader != "arrow"
     # Also old versions of arrow didn't accept gzip csv?
     params[!drop,]
   }
)

#' Get a CSV reader
#'
#' @param reader
#'
#' @return
#' @export
get_csv_reader <- function(reader) {
  library(reader, character.only = TRUE)
  if (reader == "arrow") {
    # TODO: if gzipped and arrow csv reader version doesn't support, unzip?
    return(function(...) arrow::read_csv_arrow(...))
  } else if (reader == "readr") {
    return(function(..., as_data_frame) readr::read_csv(...))
  } else if (reader == "data.table") {
    return(function(..., as_data_frame) data.table::fread(...))
  } else if (reader == "vroom") {
    # altrep = FALSE because otherwise you aren't getting the data
    # TODO maybe we do want to compare, esp. later when we do altrep
    return(function(..., as_data_frame) vroom::vroom(..., altrep = FALSE))
  } else {
    stop("Unsupported reader: ", reader, call. = FALSE)
  }
}

