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
