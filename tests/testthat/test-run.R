wipe_results <- function() unlink(test_path("results/placebo"))

test_that("run_iteration", {
  b <- Benchmark("test")
  out <- run_iteration(b, ctx = new.env())
  expect_s3_class(out, "data.frame")
  expect_identical(nrow(out), 1L)
})

test_that("run_bm", {
  b <- Benchmark("test",
    setup = function(param1 = c("a", "b")) {
      BenchEnvironment(param1 = match.arg(param1))
    },
    before_each = result <- NA,
    run = result <- param1 == "a",
    after_each = {
      stopifnot(isTRUE(result))
      rm(result)
    }
  )
  out <- run_bm(b, n_iter = 3)
  expect_s3_class(out, "arrowbench_result")
  expect_identical(nrow(out$result), 3L)

  expect_error(run_bm(b, param1 = "b"), "isTRUE(result) is not TRUE", fixed = TRUE)
})


test_that("run_one", {
  run_one(placebo)
  wipe_results()
})

test_that("Argument validation", {
  expect_message(
    run_one(placebo, not_an_arg = 1, cpu_count = 1),
    "Error.*unused argument.*not_an_arg"
  )

  expect_message(
    run_one(placebo, cpu_count = 1),
    NA
  )
  wipe_results()
})

wipe_results()