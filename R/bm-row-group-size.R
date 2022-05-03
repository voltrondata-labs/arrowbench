#' Benchmark effect of parquet row group size
#'
#' @section Parameters:
#' * `source` A known-file id, or a file path to read in
#' * `chunk_size` Number of rows to write in each row group. Suggested sizes:
#'     `chunk_size = list(NULL, 10000L, 100000L, 1000000L)`
#'
#' @export
row_group_size <- Benchmark(
  "row_group_size",
  setup = function(source = "fanniemae_2016Q4",  # TODO implement more sources
                   chunk_size = NULL) {
    # ensure that we have the right kind of file available
    input_file <- ensure_format(
      name = source, format = "parquet", compression = "snappy", chunk_size = chunk_size
    )

    library("dplyr", warn.conflicts = FALSE)

    # put the necessary variables into a BenchmarkEnvironment to be used when the
    # benchmark is running.
    BenchEnvironment(input_file = input_file)
  },

  # delete the results before each iteration
  before_each = {
    result <- NULL
    result_dim <- NULL
  },
  # the benchmark to run
  run = {
    ds <- arrow::open_dataset(input_file)

    if (grepl('fanniemae_2016Q4', input_file)) {
      result <- ds %>%
        filter(
          is.na(f2),
          f3 < 2
          | f5 > 55
          | f6 < 50
          | f8 %in% c('02/2050', '10/2059', '02/2052')
          | f14 == '08/01/2018'
          | f17 > 10000
          | f18 > 20000
          | f19 > 3000
          | f20 > 5000
          | f21 > 10000
          | f22 > 3e5
          | f23 > 1e5
          | f25 > 1000
          | f26 > 5e4
        ) %>%
        collect()

      result_dim <- c(514L, 31L)
    }
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    stopifnot(identical(dim(result), result_dim))
    result <- NULL
    result_dim <- NULL
  },

  packages_used = function(params) {
    c("arrow", "dplyr")
  }
)
