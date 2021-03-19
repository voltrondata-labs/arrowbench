known_compressions <- c("uncompressed", "snappy", "zstd", "gzip", "lz4")

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
  format = c("csv", "parquet", "feather", "csv.gz"),
  compression = known_compressions) {
  compression <- match.arg(compression)
  format <- match.arg(format)

  # if we get "csv.gz" split it up correctly.
  if (format == "csv.gz") {
    format <- "csv"
    compression <- "gzip"
  }

  # generate an extension based on format + compression (with special cases for
  # uncompressed and gzip)
  if (compression == "gzip") {
    ext <- paste(format, "gz", sep = ".")
  } else if (compression == "uncompressed") {
    ext <- format
  } else {
    ext <- paste(format, compression, sep = ".")
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
  if(format == "csv" & file_ext(file_in) == "csv" ) {
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
  validate_format(format, compression)

  # read the data in
  # TODO: read in things that are easier to read in feather > parquet >> csv?
  source <- all_sources[[name]]
  tab <- source$reader(ensure_source(name))

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
#' For a given format + compression, this will `stop` if the combo is not valid
#' and will return `TRUE` invisibly if it is.
#'
#' @param format the format of the file
#' @param compression the compression codec
#'
#' @return `TRUE` invisibly
#' @keywords internal
validate_format <- function(format = c("csv", "parquet", "feather"),
                            compression = known_compressions) {
  format <- match.arg(format)
  compression <- match.arg(compression)

  valid_combos <- list(
    csv = c("uncompressed", "gzip"),
    # could add: brotli, lzo, and bz2
    parquet = c("uncompressed", "snappy", "gzip", "zstd", "lz4"),
    feather = c("uncompressed", "lz4", "zst"),
    # fst is always zstd, just a question of what level of compression, the
    # write function will use level = 0 for uncompressed and 50 for zstd
    fst = c("uncompressed", "zstd")
  )

  if (!(compression %in% valid_combos[[format]])) {
    stop("The format ", format, " does not support ", compression, " compression.", call. = FALSE)
  }

  return(invisible(TRUE))
}