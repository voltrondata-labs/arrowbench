#' @importFrom purrr map_int
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

file_with_ext <- function(file, new_ext) {
  sub(paste0(file_ext(file), "$"), new_ext, file)
}

bm_run_cache_key <- function(name, ...) {
  dots <- list(...)
  dots <- dots[sort(names(dots))]
  paste0(name, "/", paste(dots, collapse="-"))
}
