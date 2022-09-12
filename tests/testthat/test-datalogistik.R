test_that("pipx_available() works", {
  expect_equal(pipx_available(), system("which pipx") == 0L)
})

test_that("datalogistik_available() works", {
  expect_equal(datalogistik_available(), system("which datalogistik") == 0L)
})

type_floats_json <- "{\"path\": \"/Users/alistaire/.datalogistik_cache/type_floats/parquet/partitioning_0/compression_brotli\", \"name\": \"type_floats\", \"format\": \"parquet\", \"partitioning-nrows\": 0, \"dim\": [1000000, 5], \"files\": [\"type_floats.parquet\"]}"

test_that("datalogistik_generate() works", {
  mockery::stub(datalogistik_generate, "system", function(...) type_floats_json)

  expect_identical(
    datalogistik_generate("-d type_floats -f parquet -c brotli"),
    jsonlite::fromJSON(type_floats_json)
  )
})