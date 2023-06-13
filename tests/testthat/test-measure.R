test_that("with_gc_info + errors", {
  # this tests with_gc_info + errors behavior, but we can't test it quite
  # directly because of how testthat alters how errors work.

  suppress_deparse_warning(
    capture.output(
      base_error <- run_one(placebo, error_type = "base"), type = "message"
    )
  )
  expect_false(is.null(base_error$error))
  expect_match(base_error$error$error, "Error.*something went wrong \\(but I knew that\\)")

  suppress_deparse_warning(
    capture.output(
      rlang_error <- run_one(placebo, error_type = "rlang::abort"), type = "message"
    )
  )
  expect_false(is.null(base_error$error))
  expect_match(rlang_error$error$error, "Error.*something went wrong \\(but I knew that\\)")
})
