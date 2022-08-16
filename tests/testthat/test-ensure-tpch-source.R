# This test (might) include installing a custom version of DuckDB that has the
# tpc-h extension built. This doesn't work well when coverage is running, so
# skip these tests when generating coverage.
skip_on_covr()
skip_if(Sys.getenv("ARROWBENCH_TEST_CUSTOM_DUCKDB", "") == "")

temp_dir <- tempfile()
dir.create(temp_dir)

expected_filenames <- as.list(set_names(
  file.path(temp_dir, "tpc-h", "0.01", paste0(tpch_tables, "_0.01.parquet")),
  nm = tpch_tables
))

withr::with_envvar(
  list(ARROWBENCH_DATA_DIR = temp_dir),
  {

    test_that("can generate a small dataset", {
      tpch_files <- ensure_tpch(0.01)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })

    test_that("can read that same small dataset if it is in the data folder already", {
      mockery::stub(ensure_tpch, 'generate_tpch', function(scale_factor) stop("this should not be called"))
      tpch_files <- ensure_tpch(0.01)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })

    test_that("and ensure gets the same thing", {
      tpch_files <- ensure_source("tpch", scale_factor = 0.01)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })
  }
)
