#' Run an object
#'
#' @param x An S3 classed object to run
#' @param ... Additional arguments passed through to methods. For
#' `run.BenchmarkDataFrame`, passed through to [get_default_parameters()] (when
#' parameters are not specified) and [run_benchmark()].
#'
#' @return A modified object containing run results. For `run.BenchmarkDataFrame`,
#' a `results` list column is appended.
#'
#' @export
run <- function(x, ...) {
  UseMethod("run")
}


#' @export
run.default <- function(x, ...) {
  stop("No method found for class `", toString(class(x)), '`')
}


#' @param publish Flag for whether to publish results to a Conbench server. See
#' "Environment Variables" section for how to specify server details. Requires
#' the benchconnect CLI is installed; see [install_benchconnect()].
#' @param run_id Unique ID for the run. If not specified, will be generated.
#' @param run_name Name for the run. If not specified, will use `{run_reason}: {commit hash}`
#' @param run_reason Required. Low-cardinality reason for the run, e.g. "commit" or "test"
#'
#' @section Environment Variables:
#'
#' - `CONBENCH_URL`: Required. The URL of the Conbench server with no trailing
#' slash. For arrow, should be `https://conbench.ursa.dev`.
#' - `CONBENCH_EMAIL`: The email to use for Conbench login. Only required if the
#' server is private.
#' - `CONBENCH_PASSWORD`: The password to use for Conbench login. Only required
#' if the server is private.
#' - `CONBENCH_PROJECT_REPOSITORY`: The repository name (in the format
#' `org/repo`) or the URL (in the format `https://github.com/org/repo`).
#' Defaults to `"https://github.com/apache/arrow"` if unset.
#' - `CONBENCH_PROJECT_PR_NUMBER`: Recommended. The number of the GitHub pull
#' request that is running this benchmark, or `NULL` if it's a run on the
#' default branch
#' - `CONBENCH_PROJECT_COMMIT`: The 40-character commit SHA of the repo being
#' benchmarked. If missing, will attempt to obtain it from
#' `arrow::arrow_info()$build_info$git_id`, though this may not be populated
#' depending on how Arrow was built.
#' - `CONBENCH_MACHINE_INFO_NAME`: Will override detected machine host name sent
#' in `machine_info.name` when posting runs and results. Needed for cases where
#' the actual host name can vary, like CI and cloud runners.
#'
#' @rdname run
#' @export
run.BenchmarkDataFrame <- function(x,
                                   ...,
                                   publish = FALSE,
                                   run_id = NULL,
                                   run_name = NULL,
                                   run_reason = NULL) {
  # if already run (so no elements of `parameters` are NULL), is no-op
  x <- get_default_parameters(x, ...)

  if (publish) {
    stopifnot(
      "Results cannot be published without a `run_reason`!" = !is.null(run_reason)
    )

    github <- github_info()
    if (is.null(run_name)) {
      run_name <- paste(run_reason, github$commit, sep = ": ")
    }
    bm_run <- BenchmarkRun$new(
      id = run_id,
      name = run_name,
      reason = run_reason,
      github = github
    )
    start_run(run = bm_run)

    # clean up even if something fails
    on.exit({
      finish_run(run = bm_run)
    })
  }

  x$results <- purrr::map2(x$benchmark, x$parameters, function(bm, params) {
    ress <- run_benchmark(
      bm = bm,
      params = params,
      run_id = run_id,
      run_name = run_name,
      run_reason = run_reason,
      ...
    )

    if (publish) {
      ress$results <- lapply(ress$results, augment_result)
      lapply(ress$results, submit_result)
    }
    ress
  })

  x
}

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
#' @param run_name Name for the run. If not specified, will use `{run_reason}: {commit hash}`
#' @param run_reason Low-cardinality reason for the run, e.g. "commit" or "test"
#' @param run_id Unique ID for the run
#' @return A `BenchmarkResults` object, containing `results` attribute of a list
#' of length `nrow(params)`, each of those a `BenchmarkResult` object.
#' For a simpler view of results, call `as.data.frame()` on it.
#' @export
#' @importFrom purrr pmap map_lgl
#' @importFrom progress progress_bar
run_benchmark <- function(bm,
                          ...,
                          params = get_default_parameters(bm, ...),
                          n_iter = 1,
                          dry_run = FALSE,
                          profiling = FALSE,
                          read_only = FALSE,
                          run_id = NULL,
                          run_name = NULL,
                          run_reason = NULL) {
  start <- Sys.time()
  stopifnot(is.data.frame(params), nrow(params) > 0)
  message("Running ", nrow(params), " benchmarks with ", n_iter, " iterations:")
  print(params)

  batch_id <- bm$batch_id_fun(params)
  stopifnot(
    "`batch_id_fun` must return a vector of length 1 or `nrow(params)`" =
      length(batch_id) %in% c(1L, nrow(params))
  )
  params$batch_id <- batch_id

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
    run_id = run_id,
    run_name = run_name,
    run_reason = run_reason,
    test_packages = unique(bm$packages_used(params))
  )

  if (dry_run) {
    # return now so that we don't attempt to process looking for errors, etc.
    return(BenchmarkResults$new(results = out))
  }

  errors <- map_lgl(out, ~!is.null(.$error))
  if (any(errors)) {
    message(sum(errors), " benchmarks errored:")
    print(params[errors,])
  }

  if (read_only) {
    # clean out nulls from not-yet-finished benchmarks
    out <- out[!sapply(out, is.null)]
  }

  out <- BenchmarkResults$new(results = out)
  message("Total run time: ", format(Sys.time() - start))
  out
}

#' Run a Benchmark with a single set of parameters
#'
#' @inheritParams run_benchmark
#' @param batch_id a length 1 character vector to identify the batch
#' @param progress_bar a `progress` object to update progress to (default `NULL`)
#' @param run_id Unique ID for the run
#' @param run_name Name for the run
#' @param run_reason Low-cardinality reason for the run, e.g. "commit" or "test"
#' @param test_packages a character vector of packages that the benchmarks test (default `NULL`)
#' @param ... parameters passed to `bm$setup()` or global parameters; see the
#' "Parameterizing benchmarks" section of [Benchmark()]
#'
#' @return An instance of `BenchmarkResult`: an R6 object containing either
#' "stats" or "error".
#' @export
run_one <- function(bm,
                    ...,
                    n_iter = 1,
                    batch_id = NULL,
                    dry_run = FALSE,
                    profiling = FALSE,
                    progress_bar = NULL,
                    read_only = FALSE,
                    run_id = NULL,
                    run_name = NULL,
                    run_reason = NULL,
                    test_packages = NULL) {
  all_params <- list(...)

  # separate the global parameters, and make sure only those that are specified remain
  global_param_names <- c("lib_path", "cpu_count", "mem_alloc", "drop_caches")
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
    append(global_params, list(test_packages = test_packages, dry_run = dry_run, read_only = read_only))
  )

  # add in other arguments as parameters
  args <- modifyList(
    params,
    list(bm = bm, n_iter = n_iter, batch_id = batch_id, profiling = profiling,
         global_params = global_params, run_id = run_id, run_name = run_name,
         run_reason = run_reason)
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
    paste0('cat("\n', results_sentinel, '\n")'),
    "cat(out$json)",
    # The end _should_ include only the json, but sometimes `open_dataset()`
    # results in * Closing connection n being printed at the end which breaks
    # the json read in, so use an ending sentinel too.
    paste0('cat("\n', results_sentinel_end, '\n")')
  )

  if (dry_run) {
    return(script)
  }

  metadata <- assemble_metadata(
    name = bm$name,
    params = bm$tags_fun(params),
    cpu_count = global_params[["cpu_count"]],
    drop_caches = global_params[["drop_caches"]],
    n_iter = n_iter
  )

  # construct the `run_script()` arguments out of all of the params as well as a
  # few other arguments. We need all of the parameters here so that the
  # cached result file names are right. Then run the script.
  run_script_args <- modifyList(
    all_params,
    list(
      lines = script,
      name = bm$name,
      metadata = metadata,
      progress_bar = progress_bar,
      read_only = read_only,
      run_id = run_id,
      run_name = run_name,
      run_reason = run_reason
    ),
    keep.null = TRUE
  )
  result <- do.call(run_script, run_script_args)
  # If JSON reading fails because `run_bm()` errored, `batch_id` will not be populated
  if (is.null(result$batch_id)) {
    result$batch_id <- batch_id
  }
  result
}

#' Execute a benchmark run
#'
#' This is the function that gets called in the script that [run_one()] prepares.
#' You may call this function interactively, but you won't get the isolation
#' in a fresh R process that `run_one()` provides.
#' @inheritParams run_one
#' @param global_params the global parameters that have been set
#' @param run_id Unique ID for the run
#' @param run_name Name for the run
#' @param run_reason Low-cardinality reason for the run, e.g. "commit" or "test"
#' @export
#' @importFrom utils modifyList
#' @importFrom sessioninfo package_info
run_bm <- function(bm, ..., n_iter = 1, batch_id = NULL, profiling = FALSE,
                   global_params = list(), run_id = NULL, run_name = NULL, run_reason = NULL) {
  # We *don't* want to use altrep when we are setting up, or we get surprising results
  withr::with_options(
    list(arrow.use_altrep = FALSE),
    ctx <- bm$setup(...)
  )
  on.exit({
    eval(bm$teardown, envir = ctx)
    rm(ctx)
  })

  results <- list()
  for (i in seq_len(n_iter)) {
    results[[i]] <- run_iteration(
      bm = bm,
      ctx = ctx,
      profiling = profiling,
      drop_caches = global_params[["drop_caches"]]
    )
  }

  result_df <- do.call(rbind, results)

  defaults <- lapply(get_default_args(bm$setup), head, 1)
  defaults$cpu_count <- parallel::detectCores()
  params <- modifyList(defaults, list(...))

  case_version <- bm$case_version(params)
  stopifnot("Case versions may not be NA; use NULL for no versioning" = !is.na(case_version))
  params$case_version <- case_version

  all_params <- modifyList(params, global_params)
  all_params$packages <- package_info()[, c("package", "loadedversion", "date", "source")]
  names(all_params$packages)[2] <- "version"
  row.names(all_params$packages) <- NULL
  # remove `packages_info` class whose print method won't work anymore
  all_params$packages <- as.data.frame(all_params$packages)

  metadata <- assemble_metadata(
    name = bm$name,
    params = bm$tags_fun(params),
    cpu_count = global_params$cpu_count,
    drop_caches = global_params[["drop_caches"]],
    n_iter = n_iter
  )

  out <- BenchmarkResult$new(
    run_name = run_name,
    run_id = run_id,
    batch_id = batch_id,
    run_reason = run_reason,
    # let default populate
    # timestamp = utc_now_iso_format(),
    stats = list(
      data = as.list(result_df$real),
      unit = "s",
      times = list(),
      time_unit = "s",
      iterations = n_iter
    ),
    error = NULL,
    validation = NULL,
    tags = metadata$tags,
    info = metadata$info,
    optional_benchmark_info = list(
      result = result_df,
      params = all_params,
      options = metadata$options,
      output = NULL,
      rscript = NULL
    ),
    machine_info = NULL,
    cluster_info = NULL,
    context = metadata$context,
    github = metadata$github
  )

  out
}

run_iteration <- function(bm, ctx, profiling = FALSE, drop_caches = NULL) {
  eval(bm$before_each, envir = ctx)
  gc(full = TRUE)
  out <- measure(
    eval(bm$run, envir = ctx),
    profiling = profiling,
    drop_caches = drop_caches
  )
  eval(bm$after_each, envir = ctx)
  out
}

global_setup <- function(lib_path = NULL, cpu_count = NULL, mem_alloc = NULL,
                         drop_caches = NULL, test_packages = NULL,
                         dry_run = FALSE, read_only = FALSE) {
  script <- ""
  if (!dry_run & !read_only) {
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
      "options(error = function() { traceback(3); q(status = 1)})",
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
  if (!is.null(drop_caches) && drop_caches == "case") {
    script <- c(
      script,
      "sync_and_drop_caches()"
    )
  }
  script
}

#' Assemble metadata for a benchmark run
#'
#' @param name Benchmark name, i.e. `bm$name`
#' @param params Named list of parameters for the individual run, i.e. the case
#' @param cpu_count Number of CPUs allocated
#' @param drop_caches Attempt to drop the disk cache before each case or iteration.
#' Currently only works on linux. Permissible values are `"case"`, `"iteration"`,
#' and `NULL`. Defaults to `NULL`, i.e. don't drop caches.
#' @param n_iter Number of iterations
#'
#' @keywords internal
assemble_metadata <- function(name, params, cpu_count, drop_caches, n_iter) {
  tags <- params
  tags[["name"]] <- name
  tags[["dataset"]] <- params$source
  tags[["source"]] <- NULL
  tags[["cpu_count"]] <- cpu_count
  tags[["language"]] <- "R"

  arrow_info <- arrow::arrow_info()
  info <- list(
    arrow_version = arrow_info$build_info$cpp_version,
    arrow_compiler_id = arrow_info$build_info$cpp_compiler,
    arrow_compiler_version = arrow_info$build_info$cpp_compiler_version,
    benchmark_language_version = R.version.string,
    arrow_version_r = as.character(arrow_info$version)
  )

  context <- list(
    arrow_compiler_flags = arrow_info$build_info$cpp_compiler_flags,
    benchmark_language = "R"
  )

  options <- list(
    iterations = n_iter,
    drop_caches = drop_caches,
    cpu_count = cpu_count
  )

  list(
    name = name,
    tags = tags,
    info = info,
    context = context,
    github = github_info(),
    options = options
  )
}


github_info <- function() {
  repo_env <- Sys.getenv("CONBENCH_PROJECT_REPOSITORY")
  pr_number_env <- Sys.getenv("CONBENCH_PROJECT_PR_NUMBER")
  commit_env <- Sys.getenv("CONBENCH_PROJECT_COMMIT")
  github <- list(
    repository = if (repo_env != "") repo_env else "https://github.com/apache/arrow",
    commit = if (commit_env != "") commit_env else arrow::arrow_info()$build_info$git_id,
    pr_number = if (pr_number_env != "") as.integer(pr_number_env) else NULL
  )
}


#' @importFrom jsonlite fromJSON toJSON
#' @importFrom withr with_envvar
run_script <- function(lines, cmd = find_r(), ..., metadata, progress_bar, read_only = FALSE,
                       run_id = NULL, run_name = NULL, run_reason = NULL) {
  # cmd may need to vary by platform; possibly also a param for this fn?

  result_dir <- file.path(local_dir(), "results")
  if (!dir.exists(result_dir)) {
    dir.create(result_dir, recursive = TRUE)
  }
  file <- file.path(result_dir, paste0(bm_run_cache_key(...), ".json"))
  if (file.exists(file)) {
    msg <- paste0("Loading cached results: ", file)
    message(msg)
    if (!is.null(progress_bar)) {
      progress_bar$message(msg, set_width = FALSE)
      progress_bar$tick()
    }

    return(BenchmarkResult$read_json(file))
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
    # Grab the JSON
    # Keep everything after the sentinel
    result_json <- tail(result, -find_results)
    # But only that which is before the end sentinel
    result_json <- head(result_json, which(result_json == results_sentinel_end) - 1)

    # Now grab the console out
    # Keep everything before the sentinel
    result_output <- trimws(paste(
      paste0(head(result, find_results - 1), collapse = "\n"),
      "### RESULTS HAVE BEEN PARSED ###",
      paste0(tail(result, -(which(result == results_sentinel_end))), collapse = "\n"),
      sep = "\n"
    ))

    # Cache the result so we don't have to re-run it
    if (!dir.exists(dirname(file))) {
      dir.create(dirname(file))
    }
    result <- BenchmarkResult$from_json(result_json)
    if (is.null(result$optional_benchmark_info)) {
      result$optional_benchmark_info <- list()
    }
    result$optional_benchmark_info$output <- result_output
    ## add actual script
    result$optional_benchmark_info$rscript <- lines
    result$write_json(file)
  } else {
    # This means the script errored.
    message(paste(result, collapse = "\n"))
    result <- BenchmarkResult$new(
      run_name = run_name,
      run_id = run_id,
      run_reason = run_reason,
      error = list(log = result),
      tags = metadata$tags,
      info = metadata$info,
      optional_benchmark_info = list(
        params = list(...),
        options = metadata$options,
        output = NULL,
        rscript = lines
      ),
      context = metadata$context,
      github = metadata$github
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
