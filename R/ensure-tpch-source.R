#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

datalogistik_available <- function() {
  exit_code <- system("which datalogistik")

  if (exit_code != 0) {
    warning(
      'datalogistik not installed or on $PATH. If installed with pipx, ensure it ',
      'is on $PATH, e.g. by adding PATH="${PATH}:${HOME}/.local/bin" to ~/.Renviron'
    )
  }

  exit_code == 0
}

generate_tpch <- function(scale_factor = 1) {
  stopifnot(datalogistik_available())

  scale_factor_str <- format(scale_factor, scientific = FALSE)
  datalogistik_data_dir <- source_data_file("datalogistik")
  if (!dir.exists(datalogistik_data_dir)) {
    dir.create(datalogistik_data_dir)
  }

  generation_command <- paste0("datalogistik generate -d='tpc-h' -f='parquet' --bypass-cache -s=", scale_factor_str)
  command <- paste(
    paste("pushd", datalogistik_data_dir),
    generation_command,
    "popd",
    sep = " && "
  )
  exit_code <- system(command = command)
  stopifnot("datalogistik tpc-h generation failed" = exit_code == 0)

  source_dir <- source_data_file("tpc-h", scale_factor_str)
  if (!dir.exists(source_dir)) {
    dir.create(source_dir, recursive = TRUE, showWarnings = TRUE)
  }
  lapply(
    list.files(file.path(datalogistik_data_dir, "tpc-h"), full.names = TRUE, recursive = TRUE),
    function(path) {
      new_filename <- sub('.parquet', paste0("_", scale_factor_str, ".parquet"), basename(path))
      new_path <- file.path(source_dir, new_filename)

      if (file.exists(new_path)) {
        unlink(new_path, recursive = TRUE)
      }
      file.link(path, new_path)
    }
  )

  tpch_files <- list.files(source_dir, pattern = '\\.parquet$', full.names = TRUE)
  names(tpch_files) <- sub(paste0('_', scale_factor_str, '.parquet'), '', basename(tpch_files), fixed = TRUE)

  as.list(tpch_files)
}

#' @importFrom rlang set_names
ensure_tpch <- function(scale_factor = 1) {
  ensure_source_dirs_exist()

  scale_factor_str <- format(scale_factor, scientific = FALSE)
  filenames <- file.path("tpc-h", scale_factor_str, paste0(tpch_tables, "_", scale_factor_str, ".parquet"))

  # Check for places this file might already be and return those.
  cached_files <- map(filenames, data_file)
  if (all(!map_lgl(cached_files, is.null))) {
    # if the file is in our temp storage or source storage, go for it there.
    return(set_names(cached_files, nm = tpch_tables))
  }

  # generate it
  generate_tpch(scale_factor)
}