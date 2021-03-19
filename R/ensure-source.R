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

  # If the source doesn't exist we need to create it
  # Make sure data dirs exist
  if (!dir.exists(source_data_file(""))) {
    dir.create(source_data_file(""))
  }
  if (!dir.exists(temp_data_file(""))) {
    dir.create(temp_data_file(""))
  }

  known <- known_sources[[name]]
  if (!is.null(known)) {
    filename <- source_filename(name)

    # Check for places this file might already be and return those.
    cached_file <- data_file(filename)
    if (!is.null(cached_file)) {
      # if the file is in our temp storage or source storage, go for it there.
      return(cached_file)
    }

    # Look up, download it
    file <- source_data_file(filename)
    if (!file.exists(file)) {
      # override the timeout
      # TODO: retry with backoff instead of just overriding? or use `curl`?
      with_options(
        new = list(timeout = 600),
        utils::download.file(known$url, file, mode = "wb")
      )
    }
  } else if (!is.null(test_sources[[name]])) {
    test <- test_sources[[name]]
    file <- system.file("test_data", test$filename, package = "arrowbench")
  } else {
    stop(name, " is not a known source", call. = FALSE)
  }
  file
}

source_data_file <- function(...) {
  file.path(local_data_dir(), ...)
}

temp_data_file <- function(...) {
  source_data_file("temp", ...)
}

#' Find a data file
#'
#' This looks in the locations in the following order and returns the first
#' path that exists:
#'
#'   * source dir ("data")
#'   * as well as the temp directory ("data/temp")
#'
#' If there is not a file present in either of those, it returns NULL
#'
#' @param ... file path to look for
#'
#' @return path to the file (or NULL if the file doesn't exist)
#' @keywords internal
data_file <- function(...) {
  temp_file <- temp_data_file(...)
  source_file <- source_data_file(...)

  if (file.exists(temp_file)) {
    return(temp_file)
  } else if (file.exists(source_file)) {
    return(source_file)
  }

  return(NULL)
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
    path <- source_data_file(name)
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
  filename <- get_source_attr(name, "url")

  # if the filename is NULL, this is a test data source
  if (is.null(filename)) {
    filename <- get_source_attr(name, "filename")
  }

  ext <- file_ext(basename(filename))
  paste(c(name, ext), collapse = ".")
}
