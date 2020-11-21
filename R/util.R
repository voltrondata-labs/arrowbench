#' @importFrom purrr map_int
get_default_args <- function(FUN) {
  forms <- formals(FUN)
  lapply(forms[map_int(forms, length) > 1], function(x) unlist(as.list(x)[-1]))
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