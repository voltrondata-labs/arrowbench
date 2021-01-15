test_that("cache key", {
  expect_identical(bm_run_cache_key("foo", alpha = "one", beta = 2), "foo/one-2")
  expect_identical(bm_run_cache_key("foo", beta = 2, alpha = "one"), "foo/one-2")
})