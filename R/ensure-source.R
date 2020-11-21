#' Make sure a data file exists
#'
#' @param A known-source id, a file path, or a URL
#'
#' @return A valid path to a source file. If a known source but not present,
#' it will be downloaded and possibly decompressed.
#' @export
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
      utils::download.file(known$url, file, mode = "wb")
    }
    if (ext == "csv.gz") {
      if (!file.exists(file_with_ext(file, "csv"))) {
        # This could be done more efficiently
        # Could shell out to `gunzip` but that assumes the command exists
        writeLines(readLines(gzfile(file)), file_with_ext(file, "csv"))
      }
      file <- file_with_ext(file, "csv")
    }
  } else {
    stop(file, " does not exist", call. = FALSE)
  }
  file
}

data_file <- function(..., local_dir = getOption("conbench.local_dir", getwd())) {
  file.path(local_dir, "source_data", ...)
}

is_url <- function(x) is.character(x) && length(x) == 1 && grepl("://", x)

#' Read a known source
#'
#' @param file
#' @param ...
#'
#' @return
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

get_source_attr <- function(file, attr) known_sources[[file_base(file)]][[attr]]

known_sources <- list(
  fanniemae_2016Q4 = list(
    url = "https://ursa-qa.s3.amazonaws.com/fanniemae_loanperf/2016Q4.csv.gz",
    reader = function(file, ...) arrow::read_delim_arrow(file, delim = "|", col_names = FALSE, ...),
    dim = c(22180168L, 31L)
  ),
  `nyctaxi_2010-01` = list(
    url = "https://ursa-qa.s3.amazonaws.com/nyctaxi/yellow_tripdata_2010-01.csv.gz",
    reader = function(file, ...) arrow::read_csv_arrow(file, ...)
  )
)