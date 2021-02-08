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
   setup = function(source = names(known_sources),
                    reader = c("arrow", "data.table", "vroom", "readr"),
                    compression = c("uncompressed", "gzip"),
                    output = c("arrow_table", "data_frame"),
                    ...) {
     reader <- match.arg(reader)
     compression <- match.arg(compression)
     output <- match.arg(output)
     # Map string param name to function
     read_func <- get_csv_reader(reader)
     delim <- get_source_attr(source, "delim") %||% ","

     source <- ensure_source(match.arg(source))
     result_dim <- get_source_attr(source, "dim")

     input_file <- file_with_ext(source, "csv")
     if(compression == "gzip") {
       input_file <- file_with_ext(source, "csv.gz")
       if (!file.exists(input_file)) {
         # compress if the file doesn't already exist
         gzip(file_with_ext(source, "csv"), input_file)
       }
       # Check file extension, compress if not found
       input_file <- input_file
     } else {
       # Check file extension, decompress if not found
       input_file <- file_with_ext(source, "csv")
     }

     BenchEnvironment(
       # Map string param name to function
       read_func = get_csv_reader(reader),
       input_file = input_file,
       result_dim = result_dim,
       as_data_frame = output == "data_frame",
       delim = delim
     )
   },
   before_each = {
     result <- NULL
   },
   run = {
     result <- read_func(input_file, delim = delim, as_data_frame = as_data_frame)
   },
   after_each = {
     correct_format <- FALSE
     if (as_data_frame) {
       correct_format <- inherits(result, "data.frame")
     } else {
       correct_format <- inherits(result, c("Table", "ArrowObject"))
     }

     stopifnot(
       # we have a tolerance of 1 here because vroom reads 1 additional row of
       # all NAs since there are two new lines after the header
       "The dimensions do not match" = all.equal(dim(result), result_dim, tolerance = 1),
       "The format isn't correct" = correct_format
       )
     if (!as_data_frame) {
       # clean up with arrow_tables
       # this shouldn't be necessary, but without it each arrow_table iteration
       # takes longer than the previous
       # result$invalidate() is more to-the-point, but was only introduced
       # in 3.0.0, so use one of the other work arounds:
       empty_tab <- Table$create(data.frame())
     }
     result <- NULL
   },
   valid_params = function(params) {
     # on macOS data.table doesn't (typically) have multi core support
     # TODO: check if this is actually enabled before running?
     drop <- params$output == "arrow_table" & params$reader != "arrow" |
       params$reader == "readr" & params$cpu_count > 1 |
       # compression was only supported from arrow 1.0.0 and onward
       params$compression != "uncompressed" & params$reader == "arrow" & params$lib_path < "1.0"
     # Also old versions of arrow didn't accept gzip csv1
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
  # TODO: allow other readers to read non-comma delimed files
  if (reader == "arrow") {
    # TODO: if gzipped and arrow csv reader version doesn't support, unzip?
    return(function(...) arrow::read_delim_arrow(...))
  } else if (reader == "readr") {
    return(function(..., as_data_frame, delim) readr::read_delim(...))
  } else if (reader == "data.table") {
    return(function(..., as_data_frame, delim) data.table::fread(...))
  } else if (reader == "vroom") {
    # altrep = FALSE because otherwise you aren't getting the data
    # TODO: maybe we do want to compare, esp. later when we do altrep
    return(function(..., as_data_frame) vroom::vroom(..., altrep = FALSE))
  } else {
    stop("Unsupported reader: ", reader, call. = FALSE)
  }
}

