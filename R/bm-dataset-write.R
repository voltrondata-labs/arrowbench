#' Benchmark Taxi dataset (Parquet) re-partitioning
#'
#' @section Parameters:
#' * `columns` Names of new partition keys
#'
#' @export
dataset_repartition <- Benchmark("dataset_taxi_parquet",
  setup = function(
    partitions = list(
      list("store_and_fwd_flag", "passenger_count")
      # list("year", "month", "passenger_count")
      # list("year", "month", "day")
    )) {
    library("dplyr")
    dataset <- ensure_dataset("taxi_parquet")

    # TODO: ensure that the columns are

    BenchEnvironment(
      partitions = partitions,
      dataset = dataset
    )
  },
  before_each = {
    unlink(temp_data_file("new_dataset"), recursive = T)
  },
  run = {
    dataset %>%
      filter(year %in% c(2009, 2018)) %>%
      arrow::write_dataset(
        path = temp_data_file("new_dataset"),
        partitioning = partitions
      )
  },
  after_each = {
    unlink(temp_data_file("new_dataset"), recursive = T)
  },
  packages_used = function(params) {
    c("arrow", "dplyr")
  }
)
