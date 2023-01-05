#' Generate a dataframe of default parameters for a benchmark
#'
#' Generates a dataframe of parameter combinations for a benchmark to try based
#' on the parameter defaults of its `setup` function and supplied parameters.
#'
#' @param x An object for which to generate parameters
#' @param ... Named arguments corresponding to the parameters of `bm`'s `setup`
#' function. May also contain `cpu_count`, `lib_path`, and `mem_alloc`.
#'
#' @return For `get_default_parameters.Benchmark`, a dataframe of parameter combinations
#' to try with a column for each parameter and a row for each combination.
#'
#' @export
get_default_parameters <- function(x, ...) {
  UseMethod("get_default_parameters")
}

#' @rdname get_default_parameters
#' @export
get_default_parameters.default <- function(x, ...) {
  stop("No method found for class `", toString(class(x)), '`')
}

#' @rdname get_default_parameters
#' @export
get_default_parameters.Benchmark <- function(x, ...) {
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

#' @rdname get_default_parameters
#' @export
get_default_parameters.BenchmarkDataFrame <- function(x, ...) {
  x$parameters <- purrr::map2(x$benchmark, x$parameters, function(bm, params) {
    if (is.null(params)) {
      params <- get_default_parameters(bm, ...)
    }
    params
  })

  x
}
