test_that("pipx_available() works", {
  expect_equal(pipx_available(), system("which pipx") == 0L)
})

test_that("datalogistik_available() works", {
  expect_equal(datalogistik_available(), system("which datalogistik") == 0L)
})

type_floats_json <- "{\"path\": \"~/.datalogistik_cache/type_floats/parquet/partitioning_0/compression_brotli\", \"name\": \"type_floats\", \"format\": \"parquet\", \"partitioning-nrows\": 0, \"dim\": [1000000, 5], \"files\": [\"type_floats.parquet\"]}"

test_that("datalogistik_generate() works", {
  mockery::stub(datalogistik_generate, "system", function(...) type_floats_json)

  expect_identical(
    datalogistik_generate("-d type_floats -f parquet -c brotli"),
    jsonlite::fromJSON(type_floats_json)
  )
})

# We won't keep this in long term, but it's helpful for now
sources_to_test <- names(known_sources)
# Transition test, for source except for:
#   * tpch (cause it's different, but it actually works!)
sources_to_test <- sources_to_test[!sources_to_test %in% c("tpch")]
for (format in c("parquet", "csv")) {
  for (source in sources_to_test) {
    test_that(paste0("datalogistik transition: ", source, ", ", format), {
      if (source == "fanniemae_2016Q4" && tolower(Sys.info()[["sysname"]]) == "darwin") skip_on_ci("GHA runners return an illegal opcode error")
      if (source == "type_simple_features" && format == "csv") skip("type_simple_features can't be saved as a csv")
      if (source == "type_nested" && format == "csv") skip("type_nested can't be saved as a csv")

      source_file <- ensure_format(source, format)
      dims <- get_source_attr(source, "dim")

      if (format == "csv") {
        tab <- arrow::read_csv_arrow(source_file, as_data_frame = FALSE)
      } else {
        tab <- get_read_function(format)(source_file, as_data_frame = FALSE)
      }
      expect_identical(dim(tab), dims)

      # In CI, if we don't do this, the GitHub runner runs out of memory and is killed
      rm(tab)
      gc(full = TRUE)
      print(arrow_info())
    })
  }
}

