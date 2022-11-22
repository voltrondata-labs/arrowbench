test_that("with_gc_info + errors", {
  # this tests with_gc_info + errors behavior, but we can't test it quite
  # directly because of how testthat alters how errors work.

  capture.output(base_error <- run_one(placebo, error_type = "base"), type = "message")
  expect_false(is.null(base_error$error))
  expect_match(base_error$error$log[[1]], "Error.*something went wrong \\(but I knew that\\)")

  capture.output(rlang_error <- run_one(placebo, error_type = "rlang::abort"), type = "message")
  expect_false(is.null(rlang_error$error))
  expect_match(paste0(rlang_error$error$log, collapse = "\n"), "Error.*something went wrong \\(but I knew that\\)")
})
