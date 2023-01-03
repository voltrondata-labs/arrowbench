#' Benchmark Taxi 2013 dataset reading
#'
#' @section Parameters:
#' * `dataset` Name of dataset to use, either `taxi_2013` or `taxi_2013_sample` (for testing)
#' * `query` Name of a known query to run; see `dataset_taxi_2013$cases`
#'
#' @export
dataset_taxi_2013 <- Benchmark(
  "dataset_taxi_2013",
  setup = function(dataset = "taxi_2013",
                   query = names(dataset_taxi_2013$cases)) {
    name <- match.arg(dataset, c("taxi_2013", "taxi_2013_sample"))
    library("dplyr", warn.conflicts = FALSE)
    dataset <- ensure_source(name)
    query <- dataset_taxi_2013$cases[[match.arg(query)]]

    BenchEnvironment(
      name = name,
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
    query$assert(result, name)
  },
  cases = list(
    basic = list(
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
      assert = function(result, name) {
        stopifnot(
          identical(dim(result), c(if (name == "taxi_2013_sample") 0L else 4L, 3L)),
          identical(names(result), c("payment_type", "tip_pct", "n")),
          identical(sum(result$n), if (name == "taxi_2013_sample") 0L else 68158L)
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
      assert = function(result, name) {
        stopifnot(
          identical(dim(result), c(12L, 4L)),
          identical(names(result), c("year", "month", "total_amount", "n")),
          identical(sum(result$n), if (name == "taxi_2013_sample") 567L else 93334004L)
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
      assert = function(result, name) {
        stopifnot(
          identical(dim(result), c(if (name == "taxi_2013_sample") 2L else 5L, 3L)),
          identical(names(result), c("payment_type", "tip_pct", "n")),
          identical(sum(result$n), if (name == "taxi_2013_sample") 36L else 4797187L)
        )
      }
    ),
    dims = list(
      query = function(ds) {
        dim(ds)
      },
      assert = function(result, name) {
        stopifnot("dims do not match" = identical(result, c(if (name == "taxi_2013_sample") 1000L else 173179759L, 11L)))
      }
    )
  ),
  packages_used = function(params) {
    c("arrow", "dplyr")
  }
)
