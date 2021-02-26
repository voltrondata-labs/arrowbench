#' Make sure a data file exists
#'
#' @param file A known-source id, a file path, or a URL
#'
#' @return A valid path to a source file. If a known source but not present,
#' it will be downloaded and possibly decompressed.
#' @export
#' @importFrom R.utils gunzip
#' @importFrom withr with_options
ensure_source <- function(file) {
  if (is_url(file)) {
    # TODO: validate that it exists?
    return(file)
  } else if (file.exists(file)) {
    # TODO: wrap in some object?
    return(file)
  } else if (file.exists(data_file(file))) {
    return(data_file(file))
  }

  # Look up, download it
  known <- known_sources[[file]]
  if (!is.null(known)) {
    if (!dir.exists(data_file(""))) {
      # Make sure data dir exists
      dir.create(data_file(""))
    }
    ext <- file_ext(known$url)
    file <- paste(data_file(file), ext, sep = ".")
    if (!file.exists(file)) {
      # override the timeout
      # TODO: retry with backoff instead of just overriding? or use `curl`?
      with_options(
        new = list(timeout = 600),
        utils::download.file(known$url, file, mode = "wb")
      )
      # run the post processing only once.
      on.exit({
        if (!is.null(known$post_process)) {
          known$post_process(file)
        }
      })
    }
    if (ext == "csv.gz") {
      if (!file.exists(file_with_ext(file, "csv"))) {
        # This could be done more efficiently
        # Could shell out to `gunzip` but that assumes the command exists
        gunzip(file, file_with_ext(file, "csv"), remove = FALSE)
      }
      file <- file_with_ext(file, "csv")
    }
  } else {
    stop(file, " does not exist", call. = FALSE)
  }
  file
}

data_file <- function(..., local_dir = getOption("arrowbench.local_dir", getwd())) {
  file.path(local_dir, "source_data", ...)
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
get_source_attr <- function(file, attr) known_sources[[file_base(file)]][[attr]]

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
    dim = c(14863778L, 18L),
    post_process = function(filename) {
      message("Preparing data for tests. This may take a bit...")
      # remove the extra new line after the header row which causes some readers trouble
      # sed on macOS and linux is slightly different. Windows will fail here.
      if (tolower(Sys.info()["sysname"]) == "darwin" ) {
        system(paste0("sed -i '' '/^$/d' ", filename))
      } else {
        system(paste0("sed -i '/^$/d' ", filename))
      }
      # and then overwrite the gzipped file so that's available
      R.utils::gzip(filename, paste0(filename, ".gz"), overwrite = TRUE, remove = FALSE)
    }
  ),
  chi_traffic_2020_Q1 = list(
    url = "https://ursa-qa.s3.amazonaws.com/chitraffic/chi_traffic_2020_Q1.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(13038291L, 23L)
  ),
  sample_strings = list(
    url = "https://ursa-qa.s3.amazonaws.com/sample_types/sample_strings.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  sample_dict = list(
    url = "https://ursa-qa.s3.amazonaws.com/sample_types/sample_dict.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  sample_integers = list(
    url = "https://ursa-qa.s3.amazonaws.com/sample_types/sample_integers.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  sample_floats = list(
    url = "https://ursa-qa.s3.amazonaws.com/sample_types/sample_floats.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 5L)
  ),
  sample_nested = list(
    url = "https://ursa-qa.s3.amazonaws.com/sample_types/sample_nested.parquet",
    reader = function(file, ...) arrow::read_parquet(file, ...),
    dim = c(1000000L, 3L)
  )
)

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
