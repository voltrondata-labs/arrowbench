"
This script regenerates inst/benchmarks.json with all current benchmarks. That
file is used by arrow-benchmarks-ci here:
https://github.com/voltrondata-labs/arrow-benchmarks-ci/blob/main/buildkite/benchmark/run.py
to keep track of benchmarks available in a repository.
"

arrowbench::get_package_benchmarks()$name |>
  lapply(function(name) {
    list(
      command = paste(name, "n_iter = 3", "drop_caches = 'iteration'", sep = ", "),
      name = paste0("arrowbench/", name),
      runner = "arrowbench",
      flags = list(language = "R")
    )
  }) |>
  unname() |>
  jsonlite::write_json(
    path = "inst/benchmarks.json",
    pretty = TRUE,
    auto_unbox = TRUE
  )
