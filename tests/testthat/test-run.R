wipe_results <- function() unlink(test_path("results/placebo"), recursive = TRUE)

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
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  run_one(placebo)

  wipe_results()
})

test_that("Argument validation", {
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  expect_message(
    run_one(placebo, not_an_arg = 1, cpu_count = 1),
    "Error.*unused argument.*not_an_arg"
  )

  expect_message(
    run_one(placebo, cpu_count = 1),
    NA
  )


  expect_true(file.exists(test_path("results/placebo/1.json")))
})

test_that("Path validation and redaction", {
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  expect_message(
    run_one(placebo, cpu_count = 1, grid = "not/a/file@path"),
    NA
  )

  expect_true(file.exists(test_path("results/placebo/1-not_a_file@path.json")))
})

test_that("form of the results", {
  expect_message(res <- run_benchmark(placebo, cpu_count = 1))

  results_df <- as.data.frame(res)
  expect_identical(
    results_df[,c("iteration", "cpu_count", "lib_path")],
    data.frame(
      iteration = 1L,
      cpu_count = 1L,
      lib_path = "latest"
    )
  )
  expect_true(all(
    c("real", "process", "version_arrow") %in% colnames(results_df)
  ))
})

test_that("form of the results, including output", {
  expect_message(res <- run_benchmark(placebo, cpu_count = 1, output_type = "message"))

  results_df <- as.data.frame(res)
  expect_identical(
    results_df[,c("iteration", "cpu_count", "lib_path", "output")],
    data.frame(
      iteration = 1L,
      cpu_count = 1L,
      lib_path = "latest",
      output = "A message: here's some output\n### RESULTS HAVE BEEN PARSED ###"
    )
  )
  expect_true(all(
    c("real", "process", "version_arrow") %in% colnames(results_df)
  ))

  expect_message(res <- run_benchmark(placebo, cpu_count = 1, output_type = "warning"))
  results_df <- as.data.frame(res)
  expect_identical(
    results_df$output,
    paste(
      "Warning message:",
      "In placebo_func() : A warning:here's some output",
      "",
      "### RESULTS HAVE BEEN PARSED ###",
      sep = "\n"
    )
  )

  expect_message(res <- run_benchmark(placebo, cpu_count = 1, output_type = "cat"))
  results_df <- as.data.frame(res)
  expect_identical(
    results_df$output,
    "A cat: here's some output\n### RESULTS HAVE BEEN PARSED ###"
  )
})

test_that("form of the results during a dry run", {
  res <- run_benchmark(placebo, cpu_count = 10, dry_run = TRUE)

  expect_true(all(sapply(res[[1]], class) == "character"))
  expect_true("cat(\"\n##### RESULTS FOLLOW\n\")" %in% res[[1]])
  expect_true("cat(\"\n##### RESULTS END\n\")" %in% res[[length(res)]])
})

wipe_results()
