#' Benchmark CSV reading
#'
#' @section Parameters:
#' * `source` A CSV file path to read in
#' * `reader` One of `c("arrow", "data.table", "vroom", "readr")`
#' * `compression` One of `c("uncompressed", "gzip")`
#' * `output` One of `c("arrow_table", "data_frame")`
#' * `altrep` Logical
#'
#' @export