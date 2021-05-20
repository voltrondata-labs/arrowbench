#' Remote (S3) dataset reading
#'
#' @export
remote_dataset <- Benchmark("remote_dataset",
  setup = function(source = names(known_remote_datasets)) {
    library("dplyr")
    dataset <- ensure_remote_dataset(source)

    BenchEnvironment(
      dataset = dataset
    )
  },
  before_each = {
    options("arrow.use_async" = TRUE)
    result <- NULL
  },
  run = {
    collect(dataset)
  },
  after_each = {
    # Restore default value in case local benchmarks are going to run later
    options("arrow.use_async" = FALSE)
  },
  packages_used = function(params) {
    c("arrow")
  }
)
