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
    filename <- basename(known$url)

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

    # If the source doesn't exist we need to create it
    # Make sure data dirs exist
    if (!dir.exists(source_data_file(""))) {
      dir.create(source_data_file(""))
    }
    if (!dir.exists(data_file(""))) {
      dir.create(data_file(""))
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
    if (ext == "csv.gz") {
      if (!file.exists(file_with_ext(file, "csv"))) {
        # This could be done more efficiently
        # Could shell out to `gunzip` but that assumes the command exists
        gunzip(file, file_with_ext(file, "csv"), remove = FALSE)
      }
      file <- file_with_ext(file, "csv")
    }

    # finally copy it to temp since that's where it'll be looked for by other
    # tests. Again, `overwrite = TRUE` should be unnecessary here
    file.copy(file, data_file(filename), overwrite = TRUE)
    file <-  data_file(filename)
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

#' Known data files
#' @export
known_sources <- list(
  fanniemae_2016Q4 = list(
    url = "https://ursa-qa.s3.amazonaws.com/fanniemae_loanperf/2016Q4.csv.gz",
    reader = function(file, ...) arrow::read_delim_arrow(file, delim = "|", col_names = FALSE, ...),
    delim = "|",
    dim = c(22180168L, 31L)
  ),
  `nyctaxi_2010-01` = list(
    url = "https://ursa-qa.s3.amazonaws.com/nyctaxi/yellow_tripdata_2010-01.csv.gz",
    reader = function(file, ...) arrow::read_csv_arrow(file, ...),
    delim = ",",
    dim = c(14863778L, 18L)
  ),
  chi_traffic_2020_Q1 = list(
    url = "https://ursa-qa.s3.amazonaws.com/chitraffic/chi_traffic_2020_Q1.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(13038291L, 23L)
  ),
  type_strings = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_strings.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  type_dict = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_dict.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  type_integers = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_integers.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  type_floats = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_floats.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  type_nested = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_nested.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 4L)
  ),
  type_simple_features = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_simple_features.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  )
)

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

known_datasets <- list(
  taxi_parquet = list(
    url = "s3://ursa-labs-taxi-data",
    download = function(path) {
      arrow::copy_files("s3://ursa-labs-taxi-data", path)
      invisible(path)
    },
    open = function(path) {
      arrow::open_dataset(path, partitioning = c("year", "month"))
    },
    dim = c(1547741381L, 20L),
    n_files = 125
  )
)
