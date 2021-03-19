#' @importFrom purrr map_int
#' @importFrom stats setNames
get_default_args <- function(FUN) {
  forms <- formals(FUN)
  keep <- names(forms)[map_int(forms, length) > 1]
  setNames(lapply(keep, function(x) eval(forms[[x]])), keep)
}

file_exts <- function(file) {
  unlist(strsplit(basename(file), ".", fixed = TRUE))[-1]
}

file_ext <- function(file) {
  paste(file_exts(file), collapse = ".")
}

file_base <- function(file) {
  unlist(strsplit(basename(file), ".", fixed = TRUE))[1]
}

#' Get a file with an extension
#'
#' @param file the file
#' @param new_ext the new extension
#'
#' @return the file with the new extension
#' @export
#' @keywords internal
file_with_ext <- function(file, new_ext) {
  sub(paste0(file_ext(file), "$"), new_ext, file)
}

bm_run_cache_key <- function(name, ...) {
  dots <- list(...)
  dots <- dots[sort(names(dots))]
  paste0(name, "/", paste(dots, collapse="-"))
}

#' Confirm that the memory allocator enabled
#'
#' @param mem_alloc the memory allocator to be tested (one of: "jemalloc", "mimalloc", "system)
#'
#' @return nothing
#' @export
#' @keywords internal
confirm_mem_alloc <- function(mem_alloc) {
  if (arrow::arrow_info()$memory_pool$backend_name != mem_alloc) {
    stop(
      "The memory allocator being used (",
      arrow::arrow_info()$memory_pool$backend_name,
      ") is not the same as the one requested (",
      mem_alloc,
      ")."
    )
  }
}

#' Default value for NULL
#'
#' @param a Thing to test for `NULL`-ness
#' @param b Thing to use if `a` is `NULL`
#'
#' @return `a` unless it's `NULL`, then `b`
#'
#' @name null-default
#'
#' @export
"%||%" <- function(a, b) if (!is.null(a)) a else b # nolint

is.na.null <- function(x) is.null(x) || is.na(x)

is_arrow_package <- function(params, min_version = "0.17") {
  (params$reader %||% FALSE == "arrow" | params$format %||% "fst" != "fst" ) &
    params$lib_path >= min_version
}

find_r <- function() {
  if (.Platform$OS.type == "windows") {
    file.path(R.home("bin"), "R.exe")
  } else {
    file.path(R.home("bin"), "R")
  }
}

is_macos <- function() tolower(Sys.info()["sysname"]) == "darwin"

# the local_dir is the directory where results and the r library paths will be
# stored / looked up from.
local_dir <- function() {
  Sys.getenv(
    "ARROWBENCH_LOCAL_DIR",
    unset = getwd()
  )
}

# the local_data_dir is a separate directory that the source data (and temp
# data) are. Known sources and known datasets will be saved in the root, and any
# derived data sources will be put in a directory called temp under this directory.
# The default if no options or variables are set is under the local_dir in a dir
# called source_data
local_data_dir <- function() {
  Sys.getenv(
    "ARROWBENCH_DATA_DIR",
    unset = file.path(local_dir(), "data")
  )
}
