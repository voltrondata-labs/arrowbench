skip_on_covr("This doesn't work under coverage correctly")

temp_dir <- tempfile()
dir.create(temp_dir)

expected_filenames <- as.list(set_names(
  file.path(temp_dir, paste0(tpch_tables, "_0.0001.parquet")),
  nm = tpch_tables
))

withr::with_envvar(
  list(ARROWBENCH_DATA_DIR = temp_dir),
  {

    test_that("can generate a small dataset", {
      tpch_files <- ensure_tpch(0.0001)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })

    test_that("can read that same small dataset if it is in the data folder already", {
      mockery::stub(ensure_tpch, 'generate_tpch', function(scale) stop("this should not be called"))
      tpch_files <- ensure_tpch(0.0001)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })

    test_that("and ensure gets the same thing", {
      tpch_files <- ensure_source("tpch", scale = 0.0001)
      expect_identical(
        tpch_files,
        expected_filenames
      )
    })
  }
)
