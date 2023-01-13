# Call benchconnect
#
# @param args A character vector of arguments to pass to the benchconnect binary
#
# @returns A string of stdout returned by the call
call_benchconnect <- function(args) {
  stopifnot(benchconnect_available())
  res <- processx::run(command = "benchconnect", args = args, echo_cmd = TRUE, echo = TRUE)
  message(res$stderr)
  res$stdout
}


augment_run <- function(run) {
  stdout <- call_benchconnect(c("augment", "run", "--json", run$json))
  BenchmarkRun$from_json(stdout)
}

augment_result <- function(result) {
  stdout <- call_benchconnect(c("augment", "result", "--json", result$json))
  BenchmarkResult$from_json(stdout)
}


start_run <- function(run) {
  call_benchconnect(c("start", "run", "--json", run$json))
}

submit_result <- function(result) {
  call_benchconnect(c("submit", "result", "--json", result$json))
}

finish_run <- function(run) {
  # Ed note: `run` is not used right now, but there are some things we can pass
  # here in the future, so I put it here for parallelism for now. Since it is
  # not evaluated, it doesn't need to be specified for now.
  call_benchconnect(c("finish", "run", "--json", "{}"))
}
