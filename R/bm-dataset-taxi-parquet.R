#' Benchmark Taxi dataset (Parquet) reading
#'
#' @section Parameters:
#' * `query` Name of a known query to run; see `dataset_taxi_parquet$cases`
#'
#' @export
dataset_taxi_parquet <- Benchmark("partitioned-dataset-filter",
  setup = function(query = names(dataset_taxi_parquet$cases)) {
    library("dplyr", warn.conflicts = FALSE)
    dataset <- ensure_dataset("taxi_parquet")
    query <- dataset_taxi_parquet$cases[[match.arg(query)]]

    BenchEnvironment(
      query = query,
      dataset = dataset
    )
  },
  before_each = {
    result <- NULL
  },
  run = {
    result <- query$query(dataset)
  },
  after_each = {
    query$assert(result)
  },
  tags_fun = function(params) {
    # to reproduce this: https://github.com/voltrondata-labs/benchmarks/blob/main/benchmarks/partitioned_dataset_filter_benchmark.py#L23
    params$dataset <- "dataset-taxi-parquet"
    params
  },
  cases = list(
    vignette = list(
      query = function(ds) {
        ds %>%
          filter(total_amount > 100, year == 2015) %>%
          select(tip_amount, total_amount, passenger_count) %>%
          group_by(passenger_count) %>%
          summarize(
            tip_pct = median(100 * tip_amount / total_amount),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
           identical(dim(result), c(10L, 3L)),
           identical(names(result), c("passenger_count", "tip_pct", "n")),
           identical(sum(result$n), 200807L)
         )
       }
    ),
    payment_type_3 = list(
      query = function(ds) {
        ds %>%
          filter(payment_type == "3") %>%
          select(year, month, passenger_count) %>%
          group_by(year, month) %>%
          summarize(
            total_passengers = sum(passenger_count, na.rm = TRUE),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(54L, 4L)),
          identical(names(result), c("year", "month", "total_passengers", "n")),
          identical(sum(result$n), 2412399L)
        )
      }
    ),
    # The intention of this is to filter + read from a small number of parquet
    # files (smaller than the number of threads) to see if parallelism is
    # beneficial
    small_no_files = list(
      query = function(ds) {
        ds %>%
          filter(total_amount > 20, year %in% c(2011, 2019) & month == 2) %>%
          select(tip_amount, total_amount, passenger_count) %>%
          group_by(passenger_count) %>%
          summarize(
            tip_pct = median(100 * tip_amount / total_amount),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(11L, 3L)),
          identical(names(result), c("passenger_count", "tip_pct", "n")),
          identical(sum(result$n), 3069271L)
        )
      }
    ),
    dims = list(
      query = function(ds) {
        dim(ds)
      },
      assert = function(result) {
        stopifnot("dims do not match" = identical(result, c(1547741381L, 20L)))
      }
    )
  ),
  packages_used = function(params) {
    c("arrow", "dplyr")
  }
)
