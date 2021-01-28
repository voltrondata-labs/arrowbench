#' Benchmark Taxi dataset (Parquet) reading
#'
#' @section Parameters:
#' * `query` Name of a known query to run; see `dataset_taxi_parquet$cases`
#'
#' @export
dataset_taxi_parquet <- Benchmark("dataset_taxi_parquet",
  setup = function(ctx,
                   query = names(dataset_taxi_parquet$cases),
                   ...) {
    library(dplyr)
    ctx$dataset <- ensure_dataset("taxi_parquet")
    ctx$query <- dataset_taxi_parquet$cases[[match.arg(query)]]
  },
  before_each = function(ctx) {
    ctx$result <- NULL
  },
  run = function(ctx) {
    ctx$result <- ctx$query$query(ctx$dataset)
  },
  after_each = function(ctx) {
    ctx$query$assert(ctx$result)
  },
  cases = list(
    vignette = list(
      query = function(ds) {
        ds %>%
          filter(total_amount > 100, year == 2015) %>%
          select(tip_amount, total_amount, passenger_count) %>%
          group_by(passenger_count) %>%
          collect() %>%
          summarize(
            tip_pct = median(100 * tip_amount / total_amount),
            n = n()
          )
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
          filter(payment_type == 3) %>%
          select(year, month, passenger_count) %>%
          group_by(year, month) %>%
          collect() %>%
          summarize(
            total_passengers = sum(passenger_count, na.rm = TRUE),
            n = n()
          )
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(54L, 4L)),
          identical(names(result), c("year", "month", "total_passengers", "n")),
          identical(sum(result$n), 2412399L)
        )
      }
    )
  )
)
