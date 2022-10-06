test_that("pipx_available() works", {
  expect_equal(pipx_available(), system("which pipx") == 0L)
})

test_that("datalogistik_available() works", {
  expect_equal(datalogistik_available(), system("which datalogistik") == 0L)
})

type_floats_json <- "{\"path\": \"~/.datalogistik_cache/type_floats/parquet/partitioning_0/compression_brotli\", \"name\": \"type_floats\", \"format\": \"parquet\", \"partitioning-nrows\": 0, \"dim\": [1000000, 5], \"files\": [\"type_floats.parquet\"]}"

test_that("datalogistik_get() works", {
  mockery::stub(datalogistik_get, "run", function(...) list(status = 0, stdout = type_floats_json))

  expect_identical(
    datalogistik_get("type_floats", format = "parquet", compression = "brotli"),
    jsonlite::fromJSON(type_floats_json)
  )
})

# We won't keep this in long term, but it's helpful for now
sources_to_test <- known_sources
# Transition test, for source except for:
#   * tpch (cause it's different, but it actually works!)
sources_to_test <- sources_to_test[!sources_to_test %in% c("tpch")]
for (format in c("arrow", "csv", "parquet", "ndjson")) {
  for (source in sources_to_test) {
    test_that(paste0("datalogistik transition: ", source, ", ", format), {
      # GHA runners return an illegal opcode error when saving to parquet
      if (format == "parquet" && tolower(Sys.info()[["sysname"]]) == "darwin") skip_on_ci()
      if (source == "chi_traffic_2020_Q1" && format %in% c("csv", "ndjson")) skip("chi_traffic_2020_Q1 can't be saved as a csv")
      if (source == "type_simple_features" && format %in% c("csv", "ndjson")) skip("type_simple_features can't be saved as a csv")
      if (source == "type_nested" && format %in% c("csv", "ndjson")) skip("type_nested can't be saved as a csv")
      if (source == "nyctaxi_2010-01" && format == "ndjson") skip("nyctaxi_2010-01 can't be saved as a json")
      if (source == "fanniemae_2016Q4" && format == "ndjson") skip("fanniemae_2016Q4 fills up the GH runner disk")

      from_datalogistik <- ensure_source(source, format)
      source_file <- from_datalogistik$path
      dims <- from_datalogistik$dim

      if (format == "csv" && source == "fanniemae_2016Q4") {
        tab <- arrow::read_delim_arrow(source_file, delim = "|", read_options = arrow::CsvReadOptions$create(autogenerate_column_names = TRUE), as_data_frame = FALSE)
      } else if (format == "csv" ) {
        tab <- arrow::read_csv_arrow(source_file, as_data_frame = FALSE)
      } else {
        tab <- get_read_function(format)(source_file, as_data_frame = FALSE)
      }
      expect_identical(dim(tab), dims)

      # In CI, if we don't do this, the GitHub runner runs out of memory and is killed
      rm(tab)
      gc(full = TRUE)
    })
  }
}

