#' Run a Benchmark across a range of parameters
#'
#' @param bm [Benchmark()] object
#' @param ... Optional benchmark parameters to run across
#' @param params `data.frame` of parameter combinations. By default, this will
#' be constructed from the expansion of the `...` arguments, the declared
#' parameter options in `bm$setup`, and any restrictions potentially defined in
#' `bm$valid_params()`.
#' @param n_iter integer number of iterations to replicate each benchmark
#' @param dry_run logical: just return the R source code that would be run in
#' a subprocess? Default is `FALSE`, meaning that the benchmarks will be run.
#' @param profiling Logical: collect prof info? If `TRUE`, the result data will
#' contain a `prof_file` field, which you can read in with
#' `profvis::profvis(prof_input = file)`. Default is `FALSE`
#'
#' @return A `conbench_results` object, containing a list of length `nrow(params)`,
#' each of those a `list` containing "params" and either "result" or "error".
#' For a simpler view of results, call `as.data.frame()` on it.
#' @export
#' @importFrom purrr pmap map_lgl
#' @importFrom progress progress_bar
run_benchmark <- function(bm,
                          ...,
                          params = default_params(bm, ...),
                          n_iter = 1,
                          dry_run = FALSE,
                          profiling = FALSE) {
  start <- Sys.time()
  stopifnot(is.data.frame(params))
  message("Running ", nrow(params), " benchmarks with ", n_iter, " iterations:")
  print(params)

  progress_bar <- progress_bar$new(
    format = "  [:bar] :percent in :elapsed eta: :eta",
    clear = FALSE,
    total = nrow(params)
  )
  progress_bar$tick(0)

  out <- pmap(params, run_one, bm = bm, n_iter = n_iter, dry_run = dry_run, profiling = profiling, progress_bar = progress_bar)

  errors <- map_lgl(out, ~!is.null(.$error))
  if (any(errors)) {
    message(sum(errors), " benchmarks errored:")
    print(params[errors,])
  }

  out <- lapply(out, `class<-`, "conbench_result")
  class(out) <- "conbench_results"
  message("Total run time: ", format(Sys.time() - start))
  out
}

#' Run a Benchmark with a single set of parameters
#'
#' @inheritParams run_benchmark
#' @param ... parameters passed to `bm$setup()`.
#'
#' @return A `conbench_result`: a `list` containing "params" and either
#' "result" or "error".
#' @export
run_one <- function(bm, ..., n_iter = 1, dry_run = FALSE, profiling = FALSE, progress_bar) {
  eval_script <- deparse(list(bm = bm, n_iter = n_iter, ..., profiling = profiling), control = "all")
  eval_script[1] <- sub("^list", "out <- run_bm", eval_script[1])

  script <- c(
    global_setup(...),
    eval_script,
    paste0('cat("', results_sentinel, '\n")'),
    "cat(jsonlite::toJSON(unclass(out), digits = 15))"
  )

  if (dry_run) {
    return(script)
  }

  run_script(script, ..., name = bm$name, progress_bar = progress_bar)
}

#' Execute a benchmark run
#'
#' This is the function that gets called in the script that [run_one()] prepares.
#' You may call this function interactively, but you won't get the isolation
#' in a fresh R process that `run_one()` provides.
#' @inheritParams run_one
#' @export
#' @importFrom utils modifyList
#' @importFrom sessioninfo package_info
run_bm <- function(bm, ..., n_iter = 1, profiling = FALSE) {
  ctx <- bm$setup(...)
  on.exit({
    eval(bm$teardown, envir = ctx)
    rm(ctx)
  })

  results <- list()
  for (i in seq_len(n_iter)) {
    results[[i]] <- run_iteration(bm, ctx, profiling = profiling)
  }

  defaults <- lapply(get_default_args(bm$setup), head, 1)
  defaults$cpu_count <- parallel::detectCores()
  params <- modifyList(defaults, list(...))
  params$packages <- package_info()[, c("package", "loadedversion", "date", "source")]
  names(params$packages)[2] <- "version"

  structure(list(
    result = do.call(rbind, results),
    params = params
  ), class = "conbench_result")
}

run_iteration <- function(bm, ctx, profiling = FALSE) {
  eval(bm$before_each, envir = ctx)
  gc(full = TRUE)
  out <- measure(eval(bm$run, envir = ctx), profiling = profiling)
  eval(bm$after_each, envir = ctx)
  out
}

global_setup <- function(lib_path = NULL, cpu_count = NULL, ...) {
  script <- ""
  lib_path <- ensure_lib(lib_path)
  if (!is.null(lib_path)) {
    script <- c(
      script,
      paste0('.libPaths(c("', lib_path, '", .libPaths()))')
    )
  }
  if (is.numeric(cpu_count) && cpu_count > 0) {
    script <- c(
      script,
      paste0('options(Ncpus = ', cpu_count, ')'),
      paste0('Sys.setenv(VROOM_THREADS = ', cpu_count, ')'),
      paste0('Sys.setenv(R_DATATABLE_NUM_THREADS = ', cpu_count, ')'),
      paste0('arrow:::SetCpuThreadPoolCapacity(as.integer(', cpu_count, '))')
      # This friendlier wrapper was only added in 0.17
      # paste0('arrow::set_cpu_count(', cpu_count, ')')
    )
  }
  c(script, "library(conbench)")
}

#' @importFrom jsonlite fromJSON
run_script <- function(lines, cmd = "R", ..., progress_bar) {
  # cmd may need to vary by platform; possibly also a param for this fn?
  # TODO: handle env vars in ...

  result_dir <- file.path(getOption("conbench.local_dir", "."), "results")
  if (!dir.exists(result_dir)) {
    dir.create(result_dir, recursive = TRUE)
  }
  file <- file.path(result_dir, paste0(bm_run_cache_key(...), ".json"))
  if (file.exists(file)) {
    message("Loading cached results: ", file)
    return(fromJSON(file, simplifyDataFrame = TRUE))
  } else {
    dots <- list(...)
    msg <- paste0("Running ", paste(names(dots), dots, sep="=", collapse = " "))
    progress_bar$message(msg, set_width = FALSE)
    progress_bar$tick()
  }

  result <- suppressWarnings(system(paste(cmd, "--no-save -s 2>&1"), intern = TRUE, input = lines))
  find_results <- which(result == results_sentinel)
  if (length(find_results)) {
    # Keep everything after the sentinel
    result <- tail(result, -find_results)
    # Cache the result so we don't have to re-run it
    if (!dir.exists(dirname(file))) {
      dir.create(dirname(file))
    }
    writeLines(result, file)
    result <- fromJSON(result, simplifyDataFrame = TRUE)
  } else {
    # This means the script errored.
    message(paste(result, collapse = "\n"))
    result <- list(
      error = result,
      params = list(...)
    )
  }
  result
}

results_sentinel <- "##### RESULTS FOLLOW"
