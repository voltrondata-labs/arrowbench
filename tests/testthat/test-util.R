test_that("cache key", {
  expect_identical(bm_run_cache_key("foo", alpha = "one", beta = 2), "foo/one-2")
  expect_identical(bm_run_cache_key("foo", beta = 2, alpha = "one"), "foo/one-2")
})

test_that("find_r()", {
  out <- system(paste(find_r(), "--no-save -s 2>&1"), intern = TRUE, input = "print('output')\n")
  expect_match(out, "output")

  # when system fails, there's also a warning
  expect_warning(error_out <- system(paste(find_r(), "--no-save -s 2>&1"), intern = TRUE, input = "stop('this is an error')\n"))
  expect_match(error_out[[1]], "this is an error")
})


test_that("get_default_args", {
  func <- function(
    one = 1,
    a_few = c(1, 2, 3),
    null = NULL,
    # we need to use something in the package here for environment scoping +
    # testthat reasons
    a_vector = known_sources,
    none
  ) NULL

  expect_identical(
    get_default_args(func),
    list(one = 1, a_few = c(1, 2, 3), a_vector = known_sources)
  )
})


test_that("sync_and_drop_caches() works", {
  # @param ... named values where names are values for `args` and values are
  # whether to fail
  make_mock_run_function <- function(...) {
    dots <- list(...)
    function(command, args, error_on_status) {
      list(status = as.integer(dots[[args]]))
    }
  }

  cases = purrr::cross(list(
    "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches" = c(TRUE, FALSE),
    "sync; sudo purge" = c(TRUE, FALSE)
  ))

  for (case in cases) {
    options(
      "arrowbench.drop_caches_failed" = NULL,
      "arrowbench.purge_failed" = NULL
    )

    mockery::stub(
      where = sync_and_drop_caches,
      what = "processx::run",
      how = do.call(make_mock_run_function, case)
    )

    expect_identical(sync_and_drop_caches(), any(!unlist(case)))

    if (case[["sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"]]) {
      expect_true(getOption("arrowbench.drop_caches_failed"))
      if (case[["sync; sudo purge"]]) {
        expect_true(getOption("arrowbench.purge_failed"))
      } else {
        expect_null(getOption("arrowbench.purge_failed"))
      }
    } else {
      expect_null(getOption("arrowbench.drop_caches_failed"))
      expect_null(getOption("arrowbench.purge_failed"))
    }
  }

  options(
    "arrowbench.drop_caches_failed" = NULL,
    "arrowbench.purge_failed" = NULL
  )
})