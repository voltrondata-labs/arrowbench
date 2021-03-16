test_that("cache key", {
  expect_identical(bm_run_cache_key("foo", alpha = "one", beta = 2), "foo/one-2")
  expect_identical(bm_run_cache_key("foo", beta = 2, alpha = "one"), "foo/one-2")
})

test_that("find_r()", {
  out <- system(paste(find_r(), "--no-save -s 2>&1"), intern = TRUE, input = "print('output')\n")
  expect_match(out, "output")

  error_out <- system(paste(find_r(), "--no-save -s 2>&1"), intern = TRUE, input = "stop('this is an error')\n")
  expect_match(error_out[[1]], "this is an error")
})