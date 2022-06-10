#' Benchmark Taxi 2013 dataset reading
#'
#' @section Parameters:
#' * `query` Name of a known query to run; see `dataset_taxi_2013$cases`
#'
#' @export
dataset_taxi_2013 <- Benchmark(
  "dataset_taxi_2013",
  setup = function(query = names(dataset_taxi_2013$cases)) {
    library("dplyr", warn.conflicts = FALSE)
    dataset <- ensure_dataset("taxi_2013")
    query <- dataset_taxi_2013$cases[[match.arg(query)]]

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
  cases = list(
    vignette = list(
      query = function(ds) {
        ds %>%
          filter(total_amount > 100, vendor_id == "CMT") %>%
          select(tip_amount, total_amount, payment_type) %>%
          group_by(payment_type) %>%
          summarize(
            tip_pct = median(100 * tip_amount / total_amount),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(4L, 3L)),
          identical(names(result), c("payment_type", "tip_pct", "n")),
          identical(sum(result$n), 68158L)
        )
      }
    ),
    payment_type_crd = list(
      query = function(ds) {
        ds %>%
          filter(payment_type == "CRD") %>%
          mutate(year = year(pickup_datetime), month = month(pickup_datetime)) %>%
          select(year, month, total_amount) %>%
          group_by(year, month) %>%
          summarize(
            total_amount = sum(total_amount, na.rm = TRUE),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(12L, 4L)),
          identical(names(result), c("year", "month", "total_amount", "n")),
          identical(sum(result$n), 93334004L)
        )
      }
    ),
    # The intention of this is to filter + read from a small number of csv
    # files (smaller than the number of threads) to see if parallelism is
    # beneficial
    small_no_files = list(
      query = function(ds) {
        ds %>%
          mutate(month = month(pickup_datetime)) %>%
          filter(total_amount > 20, month %in% c(4L, 7L)) %>%
          select(tip_amount, total_amount, payment_type) %>%
          group_by(payment_type) %>%
          summarize(
            tip_pct = median(100 * tip_amount / total_amount),
            n = n()
          ) %>%
          collect()
      },
      assert = function(result) {
        stopifnot(
          identical(dim(result), c(5L, 3L)),
          identical(names(result), c("payment_type", "tip_pct", "n")),
          identical(sum(result$n), 4797187L)
        )
      }
    ),
    count_rows = list(
      query = function(ds) {
        dim(ds)
      },
      assert = function(result) {
        stopifnot("dims does not match" = identical(result, c(173179759L, 11L)))
      }
    )
  ),
  packages_used = function(params) {
    c("arrow", "dplyr")
  }
)
