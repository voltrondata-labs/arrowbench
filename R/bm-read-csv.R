#' Benchmark CSV reading
#'
#' @section Parameters:
#' * `source` A CSV file path to read in
#' * `reader` One of `c("arrow", "data.table", "vroom", "readr")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output` One of `c("arrow_table", "data_frame")`
#'
#' @export
read_csv <- Benchmark("read_csv",
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
     ctx$delim <- conbench:::get_source_attr(source, "delim") %||% ","

     source <- ensure_source(source)
     ctx$result_dim <- conbench:::get_source_attr(source, "dim")

     input_file <- conbench:::file_with_ext(source, "csv")
     if(ctx$compression == "gzip") {
       input_file <- conbench:::file_with_ext(source, "csv.gz")
       if (!file.exists(input_file)) {
         # compress if the file doesn't already exist
         gzip(conbench:::file_with_ext(source, "csv"), input_file)
       }
       # Check file extension, compress if not found
       ctx$input_file <- input_file
     } else {
       # Check file extension, decompress if not found
       ctx$input_file <- conbench:::file_with_ext(source, "csv")
     }
   },
   before_each = function(ctx) {
     ctx$result <- NULL
   },
   run = function(ctx) {
     ctx$result <- ctx$read_func(ctx$input_file, delim = ctx$delim, as_data_frame = ctx$output == "data_frame")
   },
   after_each = function(ctx) {
     correct_format <- FALSE
     if (ctx$output == "data_frame") {
       correct_format <- inherits(ctx$result, "data.frame")
     } else if (ctx$output == "arrow_table") {
       correct_format <- inherits(ctx$result, c("Table", "ArrowObject"))
     }

     stopifnot(
       # we have a tolerance of 1 here because vroom reads 1 additional row of
       # all NAs since there are two new lines after the header
       "The dimensions do not match" = all.equal(dim(ctx$result), ctx$result_dim, tolerance = 1),
       "The format isn't correct" = correct_format
       )
     if (ctx$output == "arrow_table") {
       # clean up with arrow_tables
       # this shouldn't be necessary, but without it each arrow_table iteration
       # takes longer than the previous
       ctx$result$invalidate()
     }
     ctx$result <- NULL
   },
   valid_params = function(params) {
     drop <- params$output == "arrow_table" & params$reader != "arrow" |
       # on macOS data.table doesn't (typically) have multi core support
       # TODO: check if this is actually enabled before running?
       params$reader == "readr" & params$cpu_count > 1
     # Also old versions of arrow didn't accept gzip csv?
     params[!drop,]
   }
)

#' Get a CSV reader
#'
#' @param reader the reader to use
#'
#' @return the csv reader
#' @export
get_csv_reader <- function(reader) {
  library(reader, character.only = TRUE)
  if (reader == "arrow") {
    # TODO: if gzipped and arrow csv reader version doesn't support, unzip?
    return(function(...) arrow::read_delim_arrow(...))
  } else if (reader == "readr") {
    return(function(..., as_data_frame) readr::read_csv(...))
  } else if (reader == "data.table") {
    return(function(..., as_data_frame) data.table::fread(...))
  } else if (reader == "vroom") {
    # altrep = FALSE because otherwise you aren't getting the data
    # TODO: maybe we do want to compare, esp. later when we do altrep
    # specify the delim (only needed for compression)
    return(function(..., as_data_frame) vroom::vroom(..., altrep = FALSE, delim = ","))
  } else {
    stop("Unsupported reader: ", reader, call. = FALSE)
  }
}

