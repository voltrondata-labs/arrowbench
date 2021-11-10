#' Table names for TPC-H benchmarks
#'
#' @keywords internal
#' @export
tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

generate_tpch <- function(scale_factor = 1) {
  # Ensure that we have our custom duckdb that has the TPC-H extension built.
  ensure_custom_duckdb()

  con <- DBI::dbConnect(duckdb::duckdb("TPCH_TEMP"))
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink("TPCH_TEMP")
  }, add = TRUE)

  # set the max memory to a bit smaller than max memory so that we don't swap or oom
  tryCatch({
    mem_limit <- max_memory(minus_gb = 3)
    DBI::dbExecute(con, paste0("PRAGMA memory_limit='", mem_limit, "'"))
    message(paste0("Set max memory to: ", mem_limit))
  })
  DBI::dbExecute(con, paste0("CALL dbgen(sf=", scale_factor, ");"))

  out <- lapply(tpch_tables, function(name) {
    filename <- normalizePath(source_data_file(paste0(
      name,
      "_",
      format(scale_factor, scientific = FALSE),
      ".parquet"
    )))
    res <- DBI::dbExecute(con, paste0("COPY (SELECT * FROM ", name, ") TO '", filename, "' (FORMAT 'parquet');"))

    filename
  })

  set_names(out, tpch_tables)
}

#' @importFrom rlang set_names
ensure_tpch <- function(scale_factor = 1) {
  ensure_source_dirs_exist()

  filenames <- paste0(paste(tpch_tables, format(scale_factor, scientific = FALSE), sep="_"), ".parquet")

  # Check for places this file might already be and return those.
  cached_files <- map(filenames, data_file)
  if (all(!map_lgl(cached_files, is.null))) {
    # if the file is in our temp storage or source storage, go for it there.
    return(set_names(cached_files, nm = tpch_tables))
  }

  # generate it
  generate_tpch(scale_factor)
}

# super hacky
max_memory <- function(minus_gb = 0) {
  osName <- Sys.info()[["sysname"]]
  if (osName == "Darwin") {
    mem_bytes <- as.numeric(system("sysctl -n hw.memsize", intern = TRUE))
    mem_bytes <- structure(mem_bytes, class="object_size")
  } else if (osName == 'Linux') {
    mem <- system("cat /proc/meminfo", intern = TRUE)
    mem <- mem[grepl("MemTotal", mem)]
    # multiply by 1024, cause it's in kb
    mem <- as.numeric(strsplit(mem, " +")[[1]][2]) * 1024
    mem_bytes <- structure(mem, class="object_size")
  } else {
    stop("Only supported on macOS and Linux")
  }

  mem_bytes <- mem_bytes - minus_gb * 2^30
  return(format(mem_bytes, units = "Gb"))
}