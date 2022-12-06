#' Benchmark file writing
#'
#' @section Parameters:
#' * `source` A known-file id, or a CSV(?) file path to read in
#' * `file_type` One of `c("parquet", "feather", "fst")`
#' * `compression` One of the values: `r paste(known_compressions, collapse = ", ")`
#' * `input_type` One of `c("arrow_table", "data_frame")`
#'
#' @export
write_file <- Benchmark("file-write",
  setup = function(source = c("fanniemae_2016Q4", "nyctaxi_2010-01"),
                   file_type = c("parquet", "feather"),
                   compression = c("uncompressed", "snappy", "lz4"),
                   input_type = c("arrow_table", "data_frame")) {
    # source defaults are retrieved from the function definition (all available
    # known_sources) and then read the source in as a data.frame
    source <- ensure_source(source)
    df <- read_source(source, as_data_frame = match.arg(input_type) == "data_frame")
    # file_type defaults to parquet or feather, but can accept fst as well
    file_type <- match.arg(file_type, c("parquet", "feather", "fst"))

    # Map string param name to functions
    get_write_func <- function(file_type, compression) {
      force(compression)
      if (file_type == "feather") {
        return(function(...) arrow::write_feather(..., compression = compression))
      } else if (file_type == "parquet") {
        return(function(...) arrow::write_parquet(..., compression = compression))
      } else if (file_type == "fst") {
        # fst is always zstd, just a question of what level of compression
        level <- ifelse(compression == "uncompressed", 0, 50)
        return(function(...) fst::write_fst(..., compress = level))
      } else {
        stop("Unsupported file_type: ", file_type, call. = FALSE)
      }
    }
    write_func <- get_write_func(file_type, compression)

    # put the necessary variables into a BenchmarkEnvironment to be used when
    # the benchmark is running.
    BenchEnvironment(
      write_func = write_func,
      file_type = file_type,
      source = source,
      df = df
    )
  },
  # delete the results before each iteration
  before_each = {
    result_file <- tempfile()
  },
  # the benchmark to run
  run = {
    write_func(df, result_file)
  },
  # after each iteration, check the dimensions and delete the results
  after_each = {
    stopifnot(file.exists(result_file))
    unlink(result_file)
  },
  # validate that the parameters given are compatible
  valid_params = function(params) {
    # make sure that the file_type and the compression is compatible
    # and fst doesn't have arrow_table input_type
    drop <- !validate_format(params$file_type, params$compression) |
      params$file_type == "fst" & params$input_type == "arrow_table"
    params[!drop,]
  },
  # packages used when specific file_types are used
  packages_used = function(params) {
    pkg_map <- c(
      "feather" = "arrow",
      "parquet" = "arrow",
      "fst" = "fst"
    )
    pkg_map[params$file_type]
  }
)
