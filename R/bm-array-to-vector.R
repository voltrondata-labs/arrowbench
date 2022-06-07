#' Benchmark for reading an Arrow array to a vector
#'
#' This flexes a lower level conversion to R data structures from Arrow data structures.
#'
#' @section Parameters:
#' * `source` A known-file id to use (it will be read in to a data.frame first)
#' * `chunked_arrays` logical, should the arrays converted be `ChunkedArrays` or `Arrays`?
#' * `exclude_nulls` logical, should any columns with any `NULL`s or `NA`s in them be removed?
#' * `alt_rep` logical, should the altrep option be set? (`TRUE` to enable it, `FALSE` to disable)
#'
#' @importFrom purrr map flatten
#' @export
array_to_vector <- Benchmark(
  "array_to_vector",
  version = "1.0.0",

  setup = function(
      # the only datasets that have any no-null numerics are
      source = c("type_integers", "type_floats"),
      chunked_arrays = FALSE,
      exclude_nulls = FALSE,
      alt_rep = TRUE
      ) {
    stopifnot(
      is.logical(chunked_arrays),
      is.logical(exclude_nulls),
      is.logical(alt_rep)
    )
    source <- match.arg(source, names(known_sources))
    source <- ensure_source(source)
    result_dim <- get_source_attr(source, "dim")
    table <- read_source(source, as_data_frame = FALSE)

    if (exclude_nulls) {
      cols_without_nulls <- unlist(lapply(colnames(table), function(x) table[[x]]$null_count == 0))
      table <- table[which(cols_without_nulls)]
      result_dim[2] <- sum(cols_without_nulls)
    }

    # extract the arrays
    arrays <- purrr::map(colnames(table), ~table[[.]])

    # If we can operate on arrays, then pull the chunks out and flatten
    if (!chunked_arrays) {
      arrays <- purrr::flatten(purrr::map(arrays, function (array) {
        n_chunks <- array$num_chunks
        purrr::map(seq_len(n_chunks) - 1L, ~array$chunk(.))
      }))
    }

    array_lengths <- lapply(arrays, function(array) array$length())

    as_vector_func <- function(array) as.vector(array)

    BenchEnvironment(
      as_vector_func = as_vector_func,
      array_lengths = array_lengths,
      arrays = arrays,
      alt_rep = alt_rep
    )
  },
  before_each = {
    result <- NULL
    options(arrow.use_altrep = alt_rep)
  },
  run = {
    result <- lapply(arrays, as_vector_func)
  },
  after_each = {
    # altrep checking
    # TODO: should we also check that one of the classes is "arrow"?
    is_altrep <- unlist(purrr::map(result, ~!is.null(.Internal(altrep_class(.)))))
    if (alt_rep) {
      altrep_ok <- all(is_altrep)
    } else {
      altrep_ok <- all(!is_altrep)
    }

    stopifnot(
      "The array lengths do not match" = all.equal(lapply(result, length), array_lengths),
      "The objects do not match the altrep parameter" = altrep_ok
      )

    # reset the altrep option
    options(arrow.use_altrep = NULL)
    result <- NULL
  },
  valid_params = function(params) {
    # TODO: only enable on >5.0.0?
    params
  },
  packages_used = function(params) "arrow"
)

