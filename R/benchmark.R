#' Define a Benchmark
#'
#' @section Evaluation:
#' A `Benchmark` is evaluated something like:
#'
#' ```
#' env <- bm$setup(param1 = "value", param2 = "value")
#' for (i in seq_len(n_iter)) {
#'   eval(bm$before_each, envir = env)
#'   measure(eval(bm$run, envir = env))
#'   eval(bm$after_each, envir = env)
#' }
#' eval(bm$teardown, envir = env)
#' ```
#'
#' Benchmarks should run a single combination of parameters. Running across
#' a range of parameter combinations is handled by the runner, not the functions
#' in the benchmark object.
#'
#' @section Parametrizing benchmarks:
#'
#' When we benchmark, we often want to run our code compared with someone else's
#' code, or we want to run our code but with different settings. There are a few
#' types of parameters that get expressed differently.
#'
#' Function parameters, including both arguments to functions we're benchmarking
#' and the choice of function themselves, are expressed as arguments to the
#' `setup()` function. They should be simple values (strings or numbers) that
#' can easily be expressed in a configuration object or file. Things like R
#' functions that are parameters should be mapped to string identifiers and
#' dereferenced inside the `setup()` function.
#'
#' Where appropriate, you should enumerate all possible parameter values and
#' use `match.arg()` in your `setup()` function to select and validate. By
#' listing the possible values in the function signature, `run_benchmark` can
#' identify the full parameter space to test by the Cartesian product of the
#' defaults; this set of default parameters can be filtered down by defining a
#' `valid_params()` function for your benchmark. If you do not provide default
#' parameter values, you will be required to specify them at runtime.
#'
#' Some global or session parameters are managed outside of the Benchmark object
#' and do not require handling inside the benchmark's functions.
#' These are options that would apply to all benchmarks. Currently supported
#' R session parameters in `run_benchmark()`:
#'
#' * `lib_path`: To test different library versions, install into different
#' lib directories and provide the directories as the `lib_path` parameters.
#' They will be passed to `.libPaths()` at the beginning of each run. The
#' default `lib_path` is "latest", which doesn't set a special library path.
#' That is, by omitting `lib_path`, you're assuming that packages have been
#' installed outside of this process.
#' * `cpu_count`: To restrict the number of threads available for computation,
#' specify an integer `cpu_count`. This sets the R `Ncpus` option, which many
#' packages follow, and also caps the `arrow` threadpool size.
#'
#' Because these parameters can alter the global session state in unpredictable
#' ways, when we run benchmarks, we always do so by calling out to a fresh R
#' subprocess. That way, there is no potential contamination.
#'
#' Any other R `options()` or environment variables that affect behavior under
#' test, specific to this Benchmark, can be specified as `setup()` parameters
#' and set inside that function. Be sure to restore any previous settings in
#' the `teardown()` function.
#'
#' @param name string identifier for the benchmark, included in results
#' @param setup function having as its arguments the benchmark parameters. See
#' the `Parametrizing benchmarks` section. This function is called once
#' to initialize the benchmark context for a given set of parameters.
#' It should return [BenchEnvironment()] with any parameter values or resources
#' that the other expressions will need to run.
#' @param before_each expression that is evaluated before every iteration. You may
#' not need to define one.
#' @param run expression that executes what we want to measure (and nothing more).
#' Only code in this function is benchmarked.
#' @param after_each expression evaluated after every iteration. You can put here
#' assertions about the result of `run()`--errors in this function will fail
#' the benchmark (not record results)
#' @param teardown expression evaluated after all iterations are complete. Use this to
#' clean up any artifacts created, for example. This function may error without
#' affecting the benchmark results
#' @param valid_params function taking a `data.frame` of setup parameters and
#' returning a `data.frame` of setup parameters. Use this to filter out invalid
#' combinations of parameters (e.g. writing a Feather file with snappy
#' compression, which is unsupported)
#' @param case_version function taking a named list of setup parameters for a
#' single case and returning an integer version for the case, or `NULL` to not
#' append a version; `NA` will raise an error. Changes to version will break
#' conbench history for a case.
#' @param batch_id_fun A unary function which takes a dataframe of parameters and
#' returns a character vector to use as `batch_id` of length 1 or `nrow(params)`
#' @param tags_fun A unary function which takes a named list of setup parameters
#' for a single case and returns a named list of tags to send to Conbench for that
#' case. Can be overwritten to add static tags or postprocess parameters into more
#' readable forms.
#' @param packages_used function taking a `data.frame` of setup parameters and
#' returning a vector of R package names required
#' @param ... additional attributes or functions, possibly called in `setup()`.
#'
#' @return A `Benchmark` object containing these functions
#' @export
Benchmark <- function(name,
                      setup = function(...) BenchEnvironment(...),
                      before_each = TRUE,
                      run = TRUE,
                      after_each = TRUE,
                      teardown = TRUE,
                      valid_params = function(params) params,
                      case_version = function(params) NULL,
                      batch_id_fun = function(params) uuid(),
                      tags_fun = function(params) params,
                      packages_used = function(params) "arrow",
                      ...) {
  stopifnot(is.character(name))
  structure(
    list(
      name = name,
      setup = setup,
      before_each = substitute(before_each),
      run = substitute(run),
      after_each = substitute(after_each),
      teardown = substitute(teardown),
      valid_params = valid_params,
      case_version = case_version,
      batch_id_fun = batch_id_fun,
      tags_fun = tags_fun,
      packages_used = packages_used,
      ...),
    class = "Benchmark"
  )
}

#' Create a test environment to run benchmarks in
#'
#' @param ... named list of parameters to set in the environment
#' @return An environment
#' @export
BenchEnvironment <- function(...) list2env(list(...))

#' Generate a dataframe of default parameters for a benchmark
#'
#' Generates a dataframe of parameter combinations for a benchmark to try based
#' on the parameter defaults of its `setup` function and supplied parameters.
#'
#' @param x An object for which to generate parameters
#' @param ... Named arguments corresponding to the parameters of `bm`'s `setup`
#' function. May also contain `cpu_count`, `lib_path`, and `mem_alloc`.
#'
#' @return For `default_params.Benchmark`, a dataframe of parameter combinations
#' to try with a column for each parameter and a row for each combination.
#'
#' @export
default_params <- function(x, ...) {
  UseMethod("default_params")
}

#' @export
default_params.default <- function(x, ...) {
  stop("No method found for class `", toString(class(x)), '`')
}

#' @export
default_params.Benchmark <- function(x, ...) {
  # This takes the expansion of the default parameters in the function signature
  # perhaps restricted by the ... params
  params <- modifyList(get_default_args(x$setup), list(...))
  if (identical(params$lib_path, "all")) {
    # Default for lib_path is just "latest", if omitted
    # "all" means all old versions
    # rev() is so we run newest first. This also means we bootstrap data fixtures
    # with newest first, so that's some assurance that older versions can read
    # what the newer libs write
    params$lib_path <- rev(c(names(arrow_version_to_date), "devel", "latest"))
  }
  if (is.null(params$cpu_count)) {
    params$cpu_count <- c(1L, parallel::detectCores())
  }
  params$stringsAsFactors <- FALSE
  out <- do.call(expand.grid, params)

  # we don't change memory allocators on non-arrow packages
  if (!is.null(params$mem_alloc)) {
    # a bit of a hack, we can test memory allocators on devel or latest, but
    # "4.0" <= "devel" and "4.0" <= "latest" are both true.
    out[!is_arrow_package(out, "4.0", x$packages_used), "mem_alloc"] <- NA
    out <- unique(out)
  }

  if (!is.null(x$valid_params)) {
    out <- x$valid_params(out)
  }
  out
}

#' Extract the parameter summary as a data.frame
#'
#' Extract a data.frame that provides the parameters used in a run and the
#' error status
#'
#' @param run An instance of `BenchmarkResults` as returned by `run_benchmark`
#' or `BenchmarkResult` as returned by `run_one` and `run_bm`
#' @return a tibble
#' @export
get_params_summary <- function(run) {
  if (!inherits(run, c("BenchmarkResults", "BenchmarkResult"))) {
    stop("run objects need to be of class BenchmarkResults or BenchmarkResult")
  }
  run$params_summary
}
