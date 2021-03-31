
#' Known formats and compressions
#'
#' These formats and compression algorithms are known to {arrowbench}. Not all of
#' them will work with all formats (in fact, parquet is the only one that
#' supports all of them).
#'
#' @name knowns
#' @export
known_compressions <- c("uncompressed", "snappy", "zstd", "gzip", "lz4", "brotli", "lzo", "bz2")

#' @rdname knowns
#' @export
known_formats <- c("csv", "parquet", "feather", "fst")

#' Ensure that a source has a specific format
#'
#' @param name name of the known source
#' @param format format to be ensured
#' @param compression compression to be ensured
#'
#' @return the file that was ensured to exist
#' @export
#'
#' @importFrom utils write.csv
ensure_format <- function(
  name,
  format = known_formats,
  compression = known_compressions) {
  compression <- match.arg(compression)
  format <- match.arg(format)

  # if we get "csv.gz" split it up correctly.
  if (format == "csv.gz") {
    format <- "csv"
    compression <- "gzip"
  }

  # generate an extension of the form .compression.format (with special cases for
  # csv: .csv for uncompressed and .csv.gz for gzip)
  if (format == "csv") {
    if (compression == "gzip") {
      ext <- paste(format, "gz", sep = ".")
    } else {
      ext <- format
    }
  } else {
    ext <- paste(compression, format, sep = ".")
  }

  # exit quickly if exists already
  file_out <- data_file(file_with_ext(source_filename(name), ext))
  if (!is.null(file_out)) {
    return(file_out)
  }

  # the file hasn't been found, so we need to create it in the temp directory
  file_out <- temp_data_file(file_with_ext(source_filename(name), ext))

  # special case if input is csv + gzip compression since we don't need to read
  # that just to compress
  file_in <- ensure_source(name)
  if(format == "csv" & ( file_ext(file_in) == "csv" | file_ext(file_in) == "csv.gz" )) {
    if(compression == "gzip" & file_ext(file_in) == "csv") {
      # compress if the file doesn't already exist
      R.utils::gzip(file_in, file_out, remove = FALSE)
      return(file_out)
    } else if(compression == "uncompressed" & file_ext(file_in) == "csv.gz") {
      # compress if the file doesn't already exist
      R.utils::gunzip(file_in, file_out, remove = FALSE)
      return(file_out)
    }
    return(file_in)
  }

  # validate that the format + compression is something that file writing knows about
  stop_if_not_valid_format(format, compression)

  # read the data in
  # TODO: read in things that are easier to read in feather > parquet >> csv?
  tab <- read_source(file_in)

  # write the reformatted data based on the format/ext
  write_func <- get_write_function(format, compression)
  write_func(tab, file_out)

  file_out
}


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
  } else if (format == "csv") {
    return(function(...) readr::write_csv(...))
    stop("Unsupported format: ", format, call. = FALSE)
  }
}

#' Validate format and compression combinations
#'
#' For a given format + compression, determine if the combination is valid.
#' `validate_format()` returns a vector of `TRUE`/`FALSE` if the formats are
#' valid. `stop_if_not_valid_format()` will stop if any of the format + compressions
#' are not valid.
#'
#' @param format the format of the file
#' @param compression the compression codec
#'
#' @return `TRUE` invisibly
#' @name validate_format
#' @keywords internal
validate_format <- Vectorize(function(format, compression) {
  format <- match.arg(format, known_formats)
  compression <- match.arg(compression, known_compressions)

  valid_combos <- list(
    csv = c("uncompressed", "gzip"),
    parquet = c("uncompressed", "snappy", "gzip", "zstd", "lz4", "brotli", "lzo", "bz2"),
    feather = c("uncompressed", "lz4", "zstd"),
    # fst is always zstd, just a question of what level of compression, the
    # write function will use level = 0 for uncompressed and 50 for zstd
    fst = c("uncompressed", "zstd")
  )
  compression %in% valid_combos[[format]]
}, vectorize.args = c("format", "compression"), USE.NAMES = FALSE)


#' @rdname validate_format
stop_if_not_valid_format <- function(format, compression) {
  is_valid <- validate_format(format, compression)

  if (any(!is_valid)) {
    stop("The format ", format, " does not support ", compression, " compression.", call. = FALSE)
  }

  invisible(is_valid)
}
