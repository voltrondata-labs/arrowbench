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
#   * fanniemae_2016Q4 is this a delim issue?
#   * nyctaxi_2010-01 is a file extension issue?
#   * tpch (cause it's different, but it actually works!)
sources_to_test <- sources_to_test[!sources_to_test %in% c("tpch", "fanniemae_2016Q4", "nyctaxi_2010-01")]
for (source in sources_to_test) {
  test_that(paste0("datalogistik transition: ", source), {

    source_file <- ensure_source(source)
    dims <- get_source_attr(source, "dim")

    tab <- get_read_function("parquet")(source_file, as_data_frame = FALSE)
    expect_identical(dim(tab), dims)
  })
}
