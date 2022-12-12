#' A vector of benchmark attribute names run on `ursa-i9-9960x`
#'
#' @export
URSA_I9_9960X_R_BENCHMARK_NAMES <- c(
  "dataframe-to-table",  # `df_to_table`
  "file-read",
  "file-write",
  "partitioned-dataset-filter",  # `dataset_taxi_parquet`
  "wide-dataframe",  # not actually an R benchmark
  "tpch"  # `tpc_h`
)
