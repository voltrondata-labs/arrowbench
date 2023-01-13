test_that("call benchconnect works", {
  expect_match(call_benchconnect("--help"), "Command line utilities for interacting with a Conbench API")
})

test_that("augment_run() works", {
  reason <- "test"
  host_name <- "fake-computer"
  github <- list(
    commit = "fake-commit",
    repository = "https://github.com/conchair/conchair",
    pr_number = "47"
  )

  unaugmented_run <- BenchmarkRun$new(reason = reason, github = NULL)
  withr::with_envvar(
    c(
      "CONBENCH_MACHINE_INFO_NAME" = host_name,
      "CONBENCH_PROJECT_REPOSITORY" = github$repository,
      "CONBENCH_PROJECT_COMMIT" = github$commit,
      "CONBENCH_PROJECT_PR_NUMBER" = github$pr_number
    ),
    { augmented_run <- augment_run(unaugmented_run) }
  )

  expect_equal(unaugmented_run$reason, reason)
  expect_equal(augmented_run$reason, reason)

  expect_null(unaugmented_run$id)
  expect_type(augmented_run$id, "character")

  expect_null(unaugmented_run$machine_info)
  expect_type(augmented_run$machine_info, "list")
  expect_type(augmented_run$machine_info$name, "character")
  expect_equal(augmented_run$machine_info$name, host_name)

  expect_null(unaugmented_run$github)
  expect_equal(augmented_run$github, github)
})


test_that("augment_result() works", {
  stats <- list(data = list(1, 2, 3), unit = "s", times = NULL, time_unit = NULL, iterations = 3)
  host_name <- "fake-computer"
  github <- list(
    commit = "fake-commit",
    repository = "conchair/conchair",
    pr_number = "47"
  )

  unaugmented_result <- BenchmarkResult$new(stats = stats, github = NULL)
  withr::with_envvar(
    c(
      "CONBENCH_MACHINE_INFO_NAME" = host_name,
      "CONBENCH_PROJECT_REPOSITORY" = github$repository,
      "CONBENCH_PROJECT_COMMIT" = github$commit,
      "CONBENCH_PROJECT_PR_NUMBER" = github$pr_number
    ),
    { augmented_result <- augment_result(unaugmented_result) }
  )

  expect_equal(unaugmented_result$timestamp, augmented_result$timestamp)

  expect_equal(unaugmented_result$stats, stats)
  expect_equal(augmented_result$stats, stats)

  expect_null(unaugmented_result$batch_id)
  expect_type(augmented_result$batch_id, "character")

  expect_null(unaugmented_result$machine_info)
  expect_type(augmented_result$machine_info, "list")
  expect_type(augmented_result$machine_info$name, "character")
  expect_equal(augmented_result$machine_info$name, host_name)

  expect_null(unaugmented_result$github)
  expect_equal(augmented_result$github, github)
})


test_that("start_run() works", {
  bm_run <- BenchmarkRun$new(
    name = "arrowbench-unit-test: 2z8c9c49a5dc4a179243268e4bb6daa5",
    reason = "arrowbench-unit-test",
    github = list(
      commit = "2z8c9c49a5dc4a179243268e4bb6daa5",
      repository = "https://github.com/conchair/conchair",
      pr_number = "47"
    )
  )

  mockery::stub(
    where = start_run,
    what = "call_benchconnect",
    how = function(args) {
      expect_identical(args, c("start", "run", "--json", bm_run$json))
    }
  )
  start_run(run = bm_run)
})


test_that("submit_result() works", {
  bm_result <- BenchmarkResult$new(
    run_name = "arrowbench-unit-test: 2z8c9c49a5dc4a179243268e4bb6daa5",
    run_reason = "arrowbench-unit-test",
    github = list(
      commit = "2z8c9c49a5dc4a179243268e4bb6daa5",
      repository = "https://github.com/conchair/conchair",
      pr_number = "47"
    ),
    stats <- list(data = list(1, 2, 3), unit = "s", times = NULL, time_unit = NULL, iterations = 3)
  )

  mockery::stub(
    where = submit_result,
    what = "call_benchconnect",
    how = function(args) {
      expect_identical(args, c("submit", "result", "--json", bm_result$json))
    }
  )
  submit_result(result = bm_result)
})


test_that("finish_run() works", {
  mockery::stub(
    where = finish_run,
    what = "call_benchconnect",
    how = function(args) {
      expect_identical(args, c("finish", "run", "--json", "{}"))
    }
  )
  finish_run()
})

unlink("benchconnect-state.json")