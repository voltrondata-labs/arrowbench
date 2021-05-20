#' Remote (S3) dataset reading
#'
#' @export
remote_dataset <- Benchmark("remote_dataset",
  setup = function(source = names(known_remote_datasets)) {
    library("dplyr")
    dataset_params <- ensure_remote_dataset(source)

    BenchEnvironment(
      dataset = dataset_params$dataset,
      expected_dim = dataset_params$expected_dim
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
    print(dim(result))
    stopifnot(
      "The dimensions do not match" = all.equal(dim(result), expected_dim)
    )
    # Restore default value in case local benchmarks are going to run later
    options("arrow.use_async" = FALSE)
  },
  packages_used = function(params) {
    c("arrow")
  }
)
