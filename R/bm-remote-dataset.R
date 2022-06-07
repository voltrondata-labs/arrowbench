#' Remote (S3) dataset reading
#'
#' @export
remote_dataset <- Benchmark(
  "remote_dataset",
  version = "1.0.0",

  setup = function(source = c("taxi_file_list_parquet", "taxi_file_list_feather")) {
    library("dplyr")
    dataset <- ensure_dataset(source, download = FALSE)
    result_dim <- get_dataset_attr(source, "dim")

    BenchEnvironment(
      dataset = dataset,
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
