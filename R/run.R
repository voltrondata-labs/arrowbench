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
#' @param read_only this will only attempt to read benchmark files and will not
#' run any that it cannot find.
#'
#' @return A `arrowbench_results` object, containing a list of length `nrow(params)`,
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
                          profiling = FALSE,
                          read_only = FALSE) {
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

  out <- pmap(
    params,
    run_one,
    bm = bm,
    n_iter = n_iter,
    dry_run = dry_run,
    profiling = profiling,
    progress_bar = progress_bar,
    read_only = read_only,
    test_packages = unique(bm$packages_used(params))
  )

  if (dry_run) {
    # return now so that we don't attempt to process looking for errors, etc.
    return(out)
  }

  errors <- map_lgl(out, ~!is.null(.$error))
  if (any(errors)) {
    message(sum(errors), " benchmarks errored:")
    print(params[errors,])
  }

  if (read_only) {
    # cleanout nulls from not-yet-finished benchmarks
    out <- out[!sapply(out, is.null)]
  }

  out <- lapply(out, `class<-`, "arrowbench_result")
  class(out) <- "arrowbench_results"
  message("Total run time: ", format(Sys.time() - start))
  out
}

#' Run a Benchmark with a single set of parameters
#'
#' @inheritParams run_benchmark
#' @param progress_bar a `progress` object to update progress to (default `NULL`)
#' @param test_packages a character vector of packages that the benchmarks test (default `NULL`)
#' @param ... parameters passed to `bm$setup()`.
#'
#' @return A `arrowbench_result`: a `list` containing "params" and either
#' "result" or "error".
#' @export
run_one <- function(bm, ..., n_iter = 1, dry_run = FALSE, profiling = FALSE, progress_bar = NULL, read_only = FALSE, test_packages = NULL) {
  all_params <- list(...)

  # separate the global parameters, and make sure only those that are specified remain
  global_param_names <- c("lib_path", "cpu_count", "mem_alloc")
  global_params <- all_params[global_param_names]
  global_params <- Filter(Negate(is.null), global_params)
  # ensure that the lib_path "latest" is always present, since that's what would
  # happen when the script runs regardless
  global_params[["lib_path"]] <- global_params[["lib_path"]] %||% "latest"

  # remove the global parameters
  params <- all_params[!names(all_params) %in% global_param_names]

  # start with the global setup with parameters that are only used at the global
  # level along with the packages needed to test
  setup_script <- do.call(
    global_setup,
    append(global_params, list(test_packages = test_packages, dry_run = dry_run))
  )

  # add in other arguments as parameters
  args <- modifyList(
    params,
    list(bm = bm, n_iter = n_iter, profiling = profiling, global_params = global_params)
  )

  # transform the arguments into a string representation that can be called in
  # a new process (and alter that slightly to call `run_bm()` and then store it
  # in `out`)
  eval_script <- deparse(args, control = "all")
  eval_script[1] <- sub("^list", "out <- run_bm", eval_script[1])

  # put together the full script from the setup, what to evaluate, and finally
  # printing the results
  script <- c(
    setup_script,
    eval_script,
    paste0('cat("', results_sentinel, '\n")'),
    # The end _should_ include only the json, but sometimes `open_dataset()`
    # results in * Closing connection n being printed at the end which breaks
    # the json read in, so use an ending sentinel too.
    paste0('cat("\n', results_sentinel_end, '\n")')
  )

  if (dry_run) {
    return(script)
  }

  # construct the `run_script()` arguments out of all of the params as well as a
  # few other arguments. We need all of the parameters here so that the
  # cached result file names are right. Then run the script.
  run_script_args <- modifyList(
    all_params,
    list(
      lines = script,
      name = bm$name,
      progress_bar = progress_bar,
      read_only = read_only
    ),
    keep.null = TRUE
  )
  do.call(run_script, run_script_args)
}

#' Execute a benchmark run
#'
#' This is the function that gets called in the script that [run_one()] prepares.
#' You may call this function interactively, but you won't get the isolation
#' in a fresh R process that `run_one()` provides.
#' @inheritParams run_one
#' @param global_params the global parameters that have been set
#' @export
#' @importFrom utils modifyList
#' @importFrom sessioninfo package_info
run_bm <- function(bm, ..., n_iter = 1, profiling = FALSE, global_params = list()) {
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
  params <- modifyList(params, global_params)
  params$packages <- package_info()[, c("package", "loadedversion", "date", "source")]
  names(params$packages)[2] <- "version"

  structure(list(
    result = do.call(rbind, results),
    params = params
  ), class = "arrowbench_result")
}

run_iteration <- function(bm, ctx, profiling = FALSE) {
  eval(bm$before_each, envir = ctx)
  gc(full = TRUE)
  out <- measure(eval(bm$run, envir = ctx), profiling = profiling)
  eval(bm$after_each, envir = ctx)
  out
}

global_setup <- function(lib_path = NULL, cpu_count = NULL, mem_alloc = NULL, test_packages = NULL, dry_run = FALSE) {
  script <- ""
  if (!dry_run) {
    lib_path <- ensure_lib(lib_path, test_packages = test_packages)
  }
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
      # TODO: only set on linux? only on binaries? This is needed for RSPM to
      # identify + serve the appropriate binaries (otherwise it falls back to
      # source)
      paste0('options(HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version$platform, R.version$arch, R.version$os)))'),
      paste0('Sys.setenv(VROOM_THREADS = ', cpu_count, ')'),
      paste0('Sys.setenv(R_DATATABLE_NUM_THREADS = ', cpu_count, ')'),
      paste0('arrow:::SetCpuThreadPoolCapacity(as.integer(', cpu_count, '))')
      # This friendlier wrapper was only added in 0.17
      # paste0('arrow::set_cpu_count(', cpu_count, ')')
    )
  }
  script <- c(script, "library(arrowbench)")
  if (!is.na.null(mem_alloc)) {
    script <- c(
      script,
      paste0("confirm_mem_alloc('", mem_alloc, "')")
    )
  }
  script
}

#' @importFrom jsonlite fromJSON
#' @importFrom withr with_envvar
run_script <- function(lines, cmd = find_r(), ..., progress_bar, read_only = FALSE) {
  # cmd may need to vary by platform; possibly also a param for this fn?

  result_dir <- file.path(local_dir(), "results")
  if (!dir.exists(result_dir)) {
    dir.create(result_dir, recursive = TRUE)
  }
  file <- file.path(result_dir, paste0(bm_run_cache_key(...), ".json"))
  if (file.exists(file)) {
    msg <- paste0("Loading cached results: ", file)
    if (!is.null(progress_bar)) {
      progress_bar$message(msg, set_width = FALSE)
      progress_bar$tick()
    }

    return(fromJSON(file, simplifyDataFrame = TRUE))
  } else if (read_only) {
    msg <- paste0("\U274C results not found: ", file)
    if (!is.null(progress_bar)) {
      progress_bar$message(msg, set_width = FALSE)
      progress_bar$tick()
    }
    # return nothing since we are only reading.
    return(NULL)
  }

  dots <- list(...)
  msg <- paste0("Running ", paste(names(dots), dots, sep="=", collapse = " "))
  if (!is.null(progress_bar)) {
    # update the progress bar message so we know what's happening
    progress_bar$message(msg, set_width = FALSE)
  }

  env_vars <- list()
  if (!is.na.null(dots$mem_alloc)) {
    env_vars <- c(env_vars, ARROW_DEFAULT_MEMORY_POOL = dots$mem_alloc)
  }

  with_envvar(
    new = env_vars,
    result <- suppressWarnings(system(paste(cmd, "--no-save -s 2>&1"), intern = TRUE, input = lines))
  )
  find_results <- which(result == results_sentinel)
  if (length(find_results)) {
    # Keep everything after the sentinel
    result <- tail(result, -find_results)
    # But only that which is before the end sentinal
    result <- head(result, which(result == results_sentinel_end) - 1)
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

  if (!is.null(progress_bar)) {
    # only tick progress after completion
    progress_bar$tick()
  }
  result
}

results_sentinel <- "##### RESULTS FOLLOW"
results_sentinel_end <- "##### RESULTS END"
