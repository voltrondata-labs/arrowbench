if (!datalogistik_available()) {
  install_datalogistik()
}

# Create a temp file for the datalogistik cache
# TODO: should we skip this if we have the env var DATALOGISTIK_CACHE set already?
datalogistik_cache <- tempfile("datalogistik_cache")
dir.create(datalogistik_cache)
datalogistik_cache <- normalizePath(datalogistik_cache, winslash = "/")

# pre-populate the cache with our test data
file.copy(
  list.files(system.file("test_datalogistik_cache", package = "arrowbench"), full.names = TRUE),
  datalogistik_cache,
  recursive = TRUE
)

Sys.setenv(DATALOGISTIK_CACHE = datalogistik_cache)