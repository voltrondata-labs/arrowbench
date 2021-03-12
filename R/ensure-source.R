#' @include known-sources.R
NULL

#' Make sure a data file exists
#'
#' @param name A known-source id, a file path, or a URL
#'
#' @return A valid path to a source file. If a known source but not present,
#' it will be downloaded and possibly decompressed.
#' @export
#' @importFrom R.utils gunzip
#' @importFrom withr with_options
ensure_source <- function(name) {
  # if this is a direct file reference, return quickly.
  if (is_url(name)) {
    # TODO: validate that it exists?
    return(name)
  } else if (file.exists(name)) {
    # TODO: wrap in some object?
    return(name)
  }

  known <- known_sources[[name]]
  if (!is.null(known)) {
    filename <- source_filename(name)

    # If the source doesn't exist we need to create it
    # Make sure data dirs exist
    if (!dir.exists(source_data_file(""))) {
      dir.create(source_data_file(""))
    }
    if (!dir.exists(data_file(""))) {
      dir.create(data_file(""))
    }

    # Check for places this file might already be and return those.
    if (file.exists(data_file(filename))) {
      # if the file is in our temp storage, go for it there.
      return(data_file(filename))
    } else if (file.exists(source_data_file(filename))) {
      # if the file is in the source data (and not the temp storage), move it
      # and return the temp storage path. `overwrite = TRUE` here is redundent
      # since if it were there the case above should have caught, but let's not
      # error if that's the case
      file.copy(source_data_file(filename), data_file(filename), overwrite = TRUE)
      return(data_file(filename))
    }

    # Look up, download it
    ext <- file_ext(filename)
    file <- source_data_file(filename)
    if (!file.exists(file)) {
      # override the timeout
      # TODO: retry with backoff instead of just overriding? or use `curl`?
      with_options(
        new = list(timeout = 600),
        utils::download.file(known$url, file, mode = "wb")
      )
    }

    # copy it to temp since that's where it'll be looked for by other
    # tests. Again, `overwrite = TRUE` should be unnecessary here
    file.copy(file, data_file(filename), overwrite = TRUE)
    file <-  data_file(filename)

    # # special case for csv.gz, unzip them proactively in ithe temp data directory
    # if (ext == "csv.gz") {
    #   if (!file.exists(file_with_ext(file, "csv"))) {
    #     # This could be done more efficiently
    #     # Could shell out to `gunzip` but that assumes the command exists
    #     gunzip(file, file_with_ext(file, "csv"), remove = FALSE)
    #   }
    #   file <- file_with_ext(file, "csv")
    # }
  } else if (!is.null(test_sources[[name]])) {
    test <- test_sources[[name]]
    file <- system.file("test_data", test$filename, package = "arrowbench")
  } else {
    stop(name, " is not a known source", call. = FALSE)
  }
  file
}

source_data_file <- function(...) {
  file.path(local_dir(), "data", ...)
}

data_file <- function(..., temp_dir = "temp") {
  file.path(local_dir(), "data", temp_dir, ...)
}

is_url <- function(x) is.character(x) && length(x) == 1 && grepl("://", x)

#' Read a known source
#'
#' @param file file to read
#' @param ... extra arguments to pass
#'
#' @return the source
#' @export
read_source <- function(file, ...) {
  reader <- get_source_attr(file, "reader")
  if (is.null(reader)) {
    # Assume CSV
    # TODO: switch based on file extension
    arrow::read_csv_arrow(file, ...)
  } else {
    reader(file, ...)
  }
}

#' Get source attributes
#'
#' @param file the file to get attributes for
#' @param attr the attribute to get
#'
#' @keywords internal
#' @export
get_source_attr <- function(file, attr) all_sources[[file_base(file)]][[attr]]

# these are similar to known_sources above, with the exception that they come
# with the package, so they have a filename instead of a url
test_sources <- list(
  fanniemae_sample = list(
    filename = "fanniemae_sample.csv",
    reader = function(file, ...) arrow::read_delim_arrow(file, delim = "|", col_names = FALSE, ...),
    delim = "|",
    dim = c(757L, 108L)
  ),
  nyctaxi_sample = list(
    filename = "nyctaxi_sample.csv",
    reader = function(file, ...) arrow::read_delim_arrow(file, ...),
    delim = ",",
    dim = c(998L,  18L)
  ),
  chi_traffic_sample = list(
    filename = "chi_traffic_sample.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000L, 23L)
  )
)

all_sources <- c(known_sources, test_sources)

#' Make sure a multi-file dataset exists
#'
#' @param name A known-dataset id. See `known_datasets`.
#' @param download logical: should the dataset be synced to the local disk
#' or queried from its remote URL. Default is `TRUE`; files are cached
#' and not downloaded if they're already found locally.
#'
#' @return An `arrow::Dataset`, validated to have the correct number of rows
#' @export
ensure_dataset <- function(name, download = TRUE) {
  if (!(name %in% names(known_datasets))) {
    stop("Unknown dataset: ", name, call. = FALSE)
  }
  known <- known_datasets[[name]]
  if (download) {
    path <- data_file(name)
    if (!(dir.exists(path) && length(dir(path, recursive = TRUE)) == known$n_files)) {
      # Only download if some/all files are missing
      known$download(path)
    }
  } else {
    path <- known$url
  }
  ds <- known$open(path)
  # stopifnot(identical(dim(ds), known$dim))
  ds
}

source_filename <- function(name) {
  ext <- file_ext(basename(get_source_attr(name, "url")))
  paste(c(name, ext), collapse = ".")
}

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
  compression = c("uncompressed", "snappy", "zstd", "gzip", "gz", "lz4")) {
  # exit quickly if exists already
  compression <- match.arg(compression)
  format <- match.arg(format)

  if (format == "csv.gz") {
    format <- "csv"
    compression <- "gzip"
  }

  if (compression == "gzip") {
    ext <- paste(format, "gz", sep = ".")
  } else if (compression == "uncompressed") {
    ext <- format
  } else {
    ext <- paste(format, compression, sep = ".")
  }

  file_out <- file_with_ext(data_file(source_filename(name)), ext)
  if (file.exists(file_out)) {
    return(file_out)
  }

  # special case if input is csv + gzip compression since we don't need to read
  # that to re-serialize
  if(file_ext(ensure_source(name)) %in% c("csv", "csv.gz") && format == "csv") {
    if(compression == "gzip") {
      # compress if the file doesn't already exist
      R.utils::gzip(file_with_ext(data_file(source_filename(name)), "csv"), file_out, remove = FALSE)
      return(file_out)
    } else {
      # compress if the file doesn't already exist
      R.utils::gunzip(file_with_ext(data_file(source_filename(name)), "csv.gz"), file_out, remove = FALSE)
      return(file_out)
    }
  }

  # read
  # TODO: read in things that are easier to read in feather > parquet >> csv?
  source <- known_sources[[name]]
  tab <- source$reader(ensure_source(name))

  if (format == "csv") {
    csv_filename <- gsub(".gz", "", file_out)
    write.csv(tab, file = csv_filename, row.names = FALSE, na = "")

    if (compression == "gzip") {
      R.utils::gzip(csv_filename, ext = "gz", remove = FALSE)
    }
  } else if (format == "parquet") {
    if (compression == "gzip") {
      stop("gzip + parquet is not a known compression", call. = FALSE)
    }
    arrow::write_parquet(tab, sink = file_out, compression = compression)
  } else if (format == "feather") {
    if (compression == "gzip") {
      stop("gzip + feather is not a known compression", call. = FALSE)
    }
    arrow::write_feather(tab, sink = file_out, compression = compression)
  }
  # TODO: fst?

  file_out
}
