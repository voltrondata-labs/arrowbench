#' Benchmark for materializing an altrep Arrow array
#'
#' This flexes a lower level conversion to R data structures from Arrow data structures.
#'
#' @section Parameters:
#' * `source` A known-file id to use (it will be read in to a data.frame first)
#' * `exclude_nulls` Logical. Remove any columns with any `NULL`s or `NA`s in them?
#' * `altrep` Logical. Use altrep storage for vectors?
#' * `subset_indices` Length-one list of vector to use to subset rows of source.
#'
#' @importFrom purrr transpose
#' @export
array_altrep_materialization <- Benchmark(
  "array_altrep_materialization",

  setup = function(source = names(known_sources),
                   exclude_nulls = FALSE,
                   altrep = TRUE,
                   subset_indices = list(1:10)) {
    stopifnot(
      is.logical(exclude_nulls),
      is.logical(altrep)
    )
    source <- match.arg(source, names(known_sources))
    subset_indices <- subset_indices[[1]]

    options(arrow.use_altrep = altrep)
    path <- ensure_format(source, format = "parquet", compression = "snappy")

    # exclude non-altrep types
    pq <- arrow::ParquetFileReader$create(path)
    table_arrow_types <- lapply(pq$GetSchema()$fields, function(x) x$type)
    arrow_altrep_types <- list(
      arrow::float64(),
      arrow::int32(),
      arrow::utf8(),
      arrow::large_utf8(),
      arrow::dictionary()
    )
    table_arrow_altrep_type_mat <- outer(
      table_arrow_types,
      arrow_altrep_types,
      Vectorize(function(x, y) x == y)
    )
    is_arrow_altrep_type <- apply(table_arrow_altrep_type_mat, 1, any)
    table <- pq$ReadTable(which(is_arrow_altrep_type) - 1L)

    if (exclude_nulls) {
      cols_without_nulls <- vapply(
        colnames(table),
        function(x) table[[x]]$null_count == 0,
        logical(1L)
      )
      table <- table[which(cols_without_nulls)]
    }

    # extract the arrays
    arrays <- as.list(table)
    array_lengths <- lengths(arrays)

    # altrep checking
    altrep_classes <- lapply(arrays, function(x) .Internal(altrep_class(x)))
    is_altrep <- vapply(altrep_classes, Negate(is.null), logical(1L))
    is_arrow <- vapply(altrep_classes, function(x) "arrow" %in% x, logical(1))
    altrep_ok <- ifelse(altrep, all(is_altrep), !any(is_altrep))
    arrow_ok <- ifelse(altrep, all(is_arrow), !any(is_arrow))

    if (altrep) {
      stopifnot(
        "The arrays are not using altrep" = altrep_ok,
        "The arrays are not using Arrow altrep classes" = arrow_ok
      )
    } else {
      stopifnot(
        "The arrays are using altrep" = altrep_ok,
        "The arrays are using Arrow altrep classes" = arrow_ok
      )
    }

    BenchEnvironment(
      array_lengths = array_lengths,
      arrays = arrays,
      subset_indices = subset_indices
    )
  },

  before_each = {
    result <- NULL
  },

  run = {
    result <- lapply(arrays, function(a) {
      list(
        # Materialize subset and full vector. Ref: https://github.com/apache/arrow/pull/11225#issuecomment-928981161
        array_subset = a[subset_indices],
        array_materialized = a[]
      )
    })
  },

  after_each = {
    result_t <- purrr::transpose(result)
    subset_altrep_classes <- lapply(result_t[["array_subset"]], function(x) .Internal(altrep_class(x)))
    materialized_altrep_classes <- lapply(result_t[["array_materialized"]], function(x) .Internal(altrep_class(x)))
    subset_is_altrep <- vapply(subset_altrep_classes, is.null, logical(1))
    materialized_is_altrep <- vapply(materialized_altrep_classes, is.null, logical(1))

    stopifnot(
      "The subset array lengths do not match" = all(lengths(result_t[["array_subset"]]) == length(subset_indices)),
      "The materialized array lengths do not match" = all.equal(lengths(result_t[["array_materialized"]]), array_lengths),
      "Subset array was not materialized from altrep" = all(subset_is_altrep),
      "Full array was not materialized from altrep" = all(materialized_is_altrep)
    )

    result <- NULL
    result_t <- NULL
  },

  valid_params = function(params) params,
  packages_used = function(params) "arrow"
)

