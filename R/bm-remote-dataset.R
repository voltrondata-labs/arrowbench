#' Remote (S3) dataset reading
#'
#' @export
remote_dataset <- Benchmark("remote_dataset",
  setup = function(source = c("taxi_file_list_parquet", "taxi_file_list_feather")) {
    library("dplyr")
    # TODO: need to add back in `download = FALSE` when datalogistik supports it
    dataset <- ensure_source(source)
    result_dim <- dataset$dim

    BenchEnvironment(
      dataset = open_dataset(dataset$path),
      expected_dim = result_dim
    )
  },
  before_each = {
    options("arrow.use_async" = TRUE)
    result <- NULL
  },
  run = {
    result <- collect(dataset)
  },
  after_each = {
    stopifnot(
      "The dimensions do not match" = all.equal(dim(result), expected_dim)
    )
  },
  packages_used = function(params) {
    c("arrow")
  }
)
