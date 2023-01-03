#' @include known-sources.R
NULL


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
known_formats <- c("parquet", "csv", "arrow", "feather", "fst", "ndjson")

#' Ensure that a source has a specific format
#'
#' @param name name of the known source
#' @param format format to be ensured
#' @param compression compression to be ensured
#' @param chunk_size the number of rows to write in each chunk
#' @param scale_factor the scale factor to use (for datasources where that is relevant)
#'
#' @return the file that was ensured to exist
#' @export
#'
#' @importFrom utils write.csv
ensure_source <- function(
    name,
    format = known_formats,
    compression = known_compressions,
    chunk_size = NULL,
    scale_factor = NULL
) {
  compression <- match.arg(compression)
  format <- match.arg(format)

  # If there is a single table, then return that one table
  # TODO: this might actually be a bad idea, honestly
  tables <- datalogistik_get(name, format = format, compression = compression, scale_factor = scale_factor)$tables
  if (length(tables) == 1) {
    return(tables[[1]])
  }

  tables
}

#' Get a writer
#'
#' @param format format to write
#' @param compression compression to use
#' @param chunk_size the size of chunks to write (default: NULL, the default for
#' the format)
#'
#' @return the write function to use
#' @export
get_write_function <- function(format, compression, chunk_size = NULL) {
  force(compression)
  if (format %in% c("feather", "arrow")) {
    return(function(...) arrow::write_feather(..., chunk_size = chunk_size %||% 65536L, compression = compression))
  } else if (format == "parquet") {
    return(function(...) arrow::write_parquet(..., chunk_size = chunk_size, compression = compression))
  } else if (format == "fst") {
    # fst is always zstd, just a question of what level of compression
    level <- ifelse(compression == "uncompressed", 0, 50)
    return(function(...) fst::write_fst(..., compress = level))
  } else if (format == "csv") {
    return(function(...) readr::write_csv(...))
  } else if (format == "ndjson") {
    fun <- function(x, path, ...) {
      if (compression == "gzip") {
        con <- gzfile(path, open = "wb")
      } else {
        con <- file(path, open = "w")
      }
      on.exit(close(con))
      x <- as.data.frame(x)
      # TODO remove after data fixed; this makes stored dims not match
      # remove null-type columns
      x <- x[vapply(x, class, character(1L)) != 'vctrs_unspecified']
      jsonlite::stream_out(as.data.frame(x), con = con, na = "null")
    }
    return(fun)
  }
  stop("Unsupported format: ", format, call. = FALSE)
}

#' Validate format and compression combinations
#'
#' For a given format + compression, determine if the combination is valid.
#' `validate_format()` returns a vector of `TRUE`/`FALSE` if the formats are
#' valid.
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
    json = c("uncompressed", "gzip"),
    parquet = c("uncompressed", "snappy", "gzip", "zstd", "lz4", "brotli", "lzo", "bz2"),
    feather = c("uncompressed", "lz4", "zstd"),
    # fst is always zstd, just a question of what level of compression, the
    # write function will use level = 0 for uncompressed and 50 for zstd
    fst = c("uncompressed", "zstd")
  )
  compression %in% valid_combos[[format]]
}, vectorize.args = c("format", "compression"), USE.NAMES = FALSE)

#' Read a known source
#'
#' @param file file to read
#' @param ... extra arguments to pass
#'
#' @return the source
#' @export
read_source <- function(file, ...) {
  extension <- file_ext(file)
  if (grepl("csv", file)) {
    arrow::read_csv_arrow(file, ...)
  } else if (grepl("parquet", extension)) {
    arrow::read_parquet(file, ...)
  }  else if (grepl("feather", extension)) {
    # TODO: other extensions?
    arrow::read_ipc_stream(file, ...)
  }
}
