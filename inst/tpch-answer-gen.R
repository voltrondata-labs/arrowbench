# ARROWBENCH_LOCAL_DIR="path/to/arrowbench/storage" Rscript inst/tpch-answer-gen.R

library(arrowbench)
library(duckdb)
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)

sf <- 1

tpch_files <- ensure_source("tpch", scale_factor = sf)

input_functions <- list()

input_functions[["dplyr"]] <- function(name) {
  file <- tpch_files[[name]]
  return(arrow::read_parquet(file, as_data_frame = TRUE))
}

input_functions[["arrow"]] <- function(name) {
  file <- tpch_files[[name]]
  return(arrow::open_dataset(file, format = "parquet"))
}

con <- dbConnect(duckdb::duckdb("temp"))
dbExecute(con, paste0("PRAGMA threads=10"))

# DuckDB tables
for (name in tpch_tables) {
  file <- path.expand(tpch_files[[name]])

  sql_query <- paste0("CREATE TABLE ", name, " AS SELECT * FROM parquet_scan('", file, "');")

  file <- tpch_files[[name]]
  dbExecute(con, sql_query)
}

input_functions[["duckdb"]] <- function(name) {
  return(dplyr::tbl(con, name))
}

# create directory to save the answers to
dir.create(glue("./answers/scale-factor-{sf}/"), recursive = TRUE)

for (q in c(1:22)) {
  message("==================================================")
  message(glue("Query: {q}"))
  message("==================================================")

  query <- q

  # grab the sql queries from github (this URL might need to be updated if their location in the repo changes.)
  sql <- paste0(httr::GET(
    glue("https://raw.githubusercontent.com/duckdb/duckdb/master/extension/tpch/dbgen/queries/q{stringr::str_pad(query, 2, pad = '0')}.sql")
  ), collapse = "\n")
  # Or if you have duckdb locally, you can:
  # sql <- paste0(readLines(
  #   glue("~/repos/duckdb/extension/tpch/dbgen/queries/q{stringr::str_pad(query, 2, pad = '0')}.sql")
  # ), collapse = "\n")

  result_dplyr <- tpc_h_queries[[query]](input_functions[["dplyr"]])
  result_arrow <- tpc_h_queries[[query]](input_functions[["arrow"]], collect_func = compute)
  result_duckdb <- as_tibble(dbGetQuery(con, sql))

  # compare the arrow results with both dplyr and duckdb versions
  print(waldo::compare(as.data.frame(result_arrow), result_dplyr, tolerance = 0.01, x_arg = "arrow", y_arg = "dplyr"))
  print(waldo::compare(as.data.frame(result_arrow), result_duckdb, tolerance = 0.01, x_arg = "arrow", y_arg = "duckdb"))

  write_parquet(result_arrow, glue("./answers/scale-factor-{sf}/tpch-q{stringr::str_pad(query, 2, pad = '0')}-sf{sf}.parquet"))
}

# clean up duckdb database file
DBI::dbDisconnect(con, shutdown = TRUE)
unlink("temp")
