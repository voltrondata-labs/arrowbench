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