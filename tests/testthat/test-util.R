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
