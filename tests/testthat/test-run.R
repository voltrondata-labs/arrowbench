old_env_vars <- sapply(
  c("CONBENCH_PROJECT_REPOSITORY", "CONBENCH_PROJECT_COMMIT", "CONBENCH_PR_NUMBER"),
  Sys.getenv,
  simplify = FALSE
)

Sys.setenv("CONBENCH_PROJECT_REPOSITORY" = "https://github.com/apache/arrow")
Sys.setenv("CONBENCH_PROJECT_COMMIT" = "fake-test-commit")
Sys.setenv("CONBENCH_PR_NUMBER" = "fake-pr-number")

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

  expect_s3_class(out, "BenchmarkResult")
  expect_identical(nrow(out$optional_benchmark_info$result), 3L)

  expect_match(run_bm(b, param1 = "b")$error$error, "isTRUE(result) is not TRUE", fixed = TRUE)
})


test_that("run_one", {
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  suppress_deparse_warning(expect_s3_class(run_one(placebo), "BenchmarkResult"))

  wipe_results()
})

test_that("cases can be versioned", {
  bm_unversioned <- Benchmark(
    "unversioned",
    setup = function(x = c('foo', 'bar')) { cat(x) }
  )
  expect_benchmark_run(res_unversioned <- run_benchmark(bm_unversioned))
  lapply(res_unversioned$results, function(result) {
    # when version is not supplied, it should not appear in tags
    expect_false("case_version" %in% names(result$tags))
  })

  bm_versioned <- Benchmark(
    "versioned",
    setup = function(x = c('foo', 'bar')) cat(x),
    case_version = function(params) c("foo" = 1L, "bar" = 2L)[params$x]
  )
  expect_benchmark_run(res_versioned <- run_benchmark(bm_versioned))
  lapply(res_versioned$results, function(result) {
    expect_true("case_version" %in% names(result$tags))

    expected_version = c(foo = 1L, bar = 2L)[[result$optional_benchmark_info$params$x]]
    expect_equal(result$tags$case_version, expected_version)
  })

  expect_error(
    expect_output(run_bm(bm_versioned, x = "novel value")),
    regexp = "[Cc]ase[ _]version"  # 3.* stopifnot doesn't pass names as messages
  )
})

test_that("get_params_summary returns a data.frame",{
  expect_benchmark_run(
      bm_success <- run_benchmark(
        placebo,
        duration = 0.01,
        grid = TRUE,
        cpu_count = 1,
        output_type = "message"
      )
  )
  success_summary <- get_params_summary(bm_success)
  expect_s3_class(success_summary, "data.frame")

  expected_summary <- dplyr::tibble(
    duration = 0.01, grid = TRUE, cpu_count = 1L, output_type = "message",
    lib_path = "latest", did_error = FALSE
  )
  expect_identical(success_summary, expected_summary)
})

test_that("get_params_summary correctly returns an error column", {
  expect_benchmark_run(
    bm_error <- run_benchmark(placebo, cpu_count = 1, output_type = "message", error_type = "abort"),
    success = FALSE
  )
  error_summary <- get_params_summary(bm_error)
  expect_true(error_summary$did_error)
})


test_that("Argument validation", {
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  suppress_deparse_warning(
    capture.output(
      expect_message(
        run_one(placebo, not_an_arg = 1, cpu_count = 1),
        "Error.*unused argument.*not_an_arg"
      ),
      type = "message"
    )
  )

  suppress_deparse_warning(
    expect_message(
      run_one(placebo, cpu_count = 1),
      NA
    )
  )

  expect_true(file.exists(test_path("results/placebo/1.json")))
})

test_that("Path validation and redaction", {
  # note: these tests will call an installed version of arrowbench as well as
  # the one being tested (e.g. when using devtools::test())
  suppress_deparse_warning(
    expect_message(
      run_one(placebo, cpu_count = 1, grid = "not/a/file@path"),
      NA
    )
  )

  expect_true(file.exists(test_path("results/placebo/1-not_a_file@path.json")))
})

test_that("form of the results", {
  expect_benchmark_run(
    res <- run_benchmark(placebo, cpu_count = 1)
  )

  results_df <- as.data.frame(res)
  expect_identical(
    results_df[,c("iteration", "cpu_count", "lib_path")],
    dplyr::tibble(
      iteration = 1L,
      cpu_count = 1L,
      lib_path = "latest"
    ),
    ignore_attr = TRUE
  )
  expect_true(all(
    c("real", "process", "version_arrow") %in% colnames(results_df)
  ))
})

test_that("form of the results, including output", {
  expect_message(
    expect_benchmark_run(
      res <- run_benchmark(placebo, cpu_count = 1, output_type = "message")
    )
  )

  results_df <- as.data.frame(res)

  expected <- dplyr::tibble(
    iteration = 1L,
    cpu_count = 1L,
    lib_path = "latest",
    output = "A message: here's some output\n\n### RESULTS HAVE BEEN PARSED ###"
  )
  # output is always a character, even < 4.0, where it would default to factors
  expected$output <- as.character(expected$output)

  expect_identical(
    results_df[, c("iteration", "cpu_count", "lib_path", "output")],
    expected,
    ignore_attr = TRUE
  )
  expect_true(all(
    c("real", "process", "version_arrow") %in% colnames(results_df)
  ))

  json_keys <- c(
    "batch_id", "timestamp", "stats", "tags", "info", "optional_benchmark_info",
    "context", "github"
  )
  expect_named(res$results[[1]]$list, json_keys, ignore.order = TRUE)

  expect_message(
    expect_benchmark_run(
      res <- run_benchmark(placebo, cpu_count = 1, output_type = "warning")
    )
  )
  results_df <- as.data.frame(res)
  # `expect_match()` instead of `expect_identical()` because a traceback for the
  # warning also gets printed
  expect_match(
    results_df$output,
    paste(
      "Warning message:",
      "In placebo_func() : A warning:here's some output",
      "",
      "### RESULTS HAVE BEEN PARSED ###",
      sep = "\n"
    ),
    fixed = TRUE
  )

  expect_message(
    expect_benchmark_run(
      res <- run_benchmark(placebo, cpu_count = 1, output_type = "cat")
    )
  )
  results_df <- as.data.frame(res)
  expect_identical(
    results_df$output,
    "A cat: here's some output\n### RESULTS HAVE BEEN PARSED ###"
  )
})

test_that("form of the results during a dry run", {
  expect_benchmark_run(
    res <- run_benchmark(placebo, cpu_count = 10, dry_run = TRUE),
    success = FALSE
  )

  expect_true(all(sapply(res$results[[1]], class) == "character"))
  expect_true("cat(\"\n##### RESULTS FOLLOW\n\")" %in% res$results[[1]])
  expect_true("cat(\"\n##### RESULTS END\n\")" %in% res$results[[length(res$results)]])
})

test_that("an rscript is added to the results object", {
  expect_benchmark_run(res <- run_benchmark(placebo, cpu_count = 1))
  expect_true(file.exists(test_path("results/placebo/1-0.01-TRUE.json")))
  expect_benchmark_run(res <- run_benchmark(placebo, cpu_count = 10, duration = 0.1))
  res_path <- test_path("results/placebo/10-0.1-TRUE.json")
  expect_true(file.exists(res_path))

  res <- jsonlite::read_json(res_path)
  expect_true("rscript" %in% names(res$optional_benchmark_info))
})


test_that("run() dispatches and run.default() errors", {
  expect_error(
    run(1),
    "No method found for class `numeric`"
  )
})

test_that("run.BenchmarkDataFrame() works", {
  bm_list <- list(placebo, placebo)
  param_list <- list(
    get_default_parameters(
      placebo,
      error = list(NULL, "rlang::abort", "base::stop"),
      output_type = list(NULL, "message", "warning", "cat"),
      cpu_count = arrow::cpu_count()
    ),
    NULL
  )
  bm_df <- BenchmarkDataFrame(benchmarks = bm_list, parameters = param_list)

  # Narrower than `suppressWarnings()`; catches all instances unlike `expect_warning()`
  withCallingHandlers(
    { bm_df_res <- run(bm_df, drop_caches = "iteration", n_iter = 3L) },
    warning = function(w) if (conditionMessage(w) == "deparse may be incomplete") {
      invokeRestart(findRestart("muffleWarning"))
    }
  )

  assert_benchmark_dataframe(
    bm_df_res,
    benchmarks = bm_list,
    parameters = list(
      param_list[[1]],
      get_default_parameters(placebo, drop_caches = "iteration", n_iter = 3L)
    )
  )
  expect_true("results" %in% names(bm_df_res))
  purrr::walk2(bm_df_res$parameters, bm_df_res$results, function(parameters, results) {
    expect_s3_class(results, c("BenchmarkResults", "Serializable", "R6"))
    expect_equal(nrow(parameters), length(results$results))
    if ("error" %in% names(parameters)) {
      # param set with some cases that will error
      purrr::walk2(parameters$error, results$results, function(err, res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        if (is.null(err)) {
          # passing case
          expect_null(res$error)
          expect_gt(res$stats$data[[1]], 0)
        } else {
          # erroring case
          expect_false(is.null(res$error))
          expect_false(is.null(res$error$error))
          expect_false(is.null(res$error$stack_trace))
          if (!is.null(res$tags$output_type) && res$tags$output_type == "warning") {
            expect_false(is.null(res$error$warnings))
            expect_identical(
              res$error$warnings[[1]]$warning,
              "simpleWarning in placebo_func(): A warning:here's some output\n"
            )
            expect_gt(length(res$error$warnings[[1]]$stack_trace), 0L)
          }
        }
      })
    } else {
      # param set with no cases that will error (includes defaults)
      purrr::walk(results$results, function(res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        expect_null(res$error)
        expect_gt(res$stats$data[[1]], 0)
      })
    }
  })
})


test_that("run.BenchmarkDataFrame() with `publish = TRUE` works (with mocking)", {
  bm_list <- list(placebo, placebo)
  param_list <- list(
    get_default_parameters(
      placebo,
      error = list(NULL, "rlang::abort", "base::stop"),
      cpu_count = arrow::cpu_count()
    ),
    NULL
  )
  bm_df <- BenchmarkDataFrame(benchmarks = bm_list, parameters = param_list)

  github <- list(
    repository = "https://github.com/conchair/conchair",
    commit = "2z8c9c49a5dc4a179243268e4bb6daa5",
    pr_number = 47L
  )
  run_id <- "fake-run-id"
  run_reason <- "mocked-arrowbench-unit-test"
  run_name <- paste(run_reason, github$commit, sep = ": ")
  host_name <- "fake-computer"



  withr::with_envvar(
    c(
      CONBENCH_PROJECT_REPOSITORY = github$repository,
      CONBENCH_PROJECT_PR_NUMBER = github$pr_number,
      CONBENCH_PROJECT_COMMIT = github$commit,
      CONBENCH_MACHINE_INFO_NAME = host_name
    ),
    {
      # Narrower than `suppressWarnings()`; catches all 5 instances unlike `expect_warning()`
      withCallingHandlers(
        {
          mockery::stub(
            where = run.BenchmarkDataFrame,
            what = "start_run",
            how = function(run) {
              run <- augment_run(run)
              expect_identical(run$github, github)
              expect_identical(run$id, run_id)
              expect_identical(run$name, run_name)
              expect_identical(run$reason, run_reason)
              expect_identical(run$machine_info$name, host_name)
            }
          )

          mockery::stub(
            where = run.BenchmarkDataFrame,
            what = "submit_result",
            how = function(result) {
              expect_identical(result$github, github)
              expect_identical(result$run_id, run_id)
              expect_identical(result$run_name, run_name)
              expect_identical(result$run_reason, run_reason)
              expect_identical(result$machine_info$name, host_name)
            }
          )

          mockery::stub(
            where = run.BenchmarkDataFrame,
            what = "finish_run",
            how = function(run) {
              run <- augment_run(run)
              expect_identical(run$github, github)
              expect_identical(run$id, run_id)
              expect_identical(run$name, run_name)
              expect_identical(run$reason, run_reason)
              expect_identical(run$machine_info$name, host_name)
            }
          )

          wipe_results()
          bm_df_res <- run(
            bm_df,
            publish = TRUE,
            run_id = run_id,
            run_name = run_name,
            run_reason = run_reason

          )
        },
        warning = function(w) if (conditionMessage(w) == "deparse may be incomplete") {
          invokeRestart(findRestart("muffleWarning"))
        }
      )
    }
  )

  assert_benchmark_dataframe(
    bm_df_res,
    benchmarks = bm_list,
    parameters = list(param_list[[1]], get_default_parameters(placebo))
  )
  expect_true("results" %in% names(bm_df_res))

  # iterate over param and res list cols
  purrr::walk2(bm_df_res$parameters, bm_df_res$results, function(parameters, results) {
    # each element should be a `BenchmarkResults` (plural!) instance
    expect_s3_class(results, c("BenchmarkResults", "Serializable", "R6"))
    # each row of the param df should correspond to a result instance
    expect_equal(nrow(parameters), length(results$results))
    # iterate over results in `BenchmarkResults` object
    if ("error" %in% names(parameters)) {
      # param set with some cases that will error
      purrr::walk2(parameters$error, results$results, function(err, res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        if (is.null(err)) {
          # passing case
          expect_null(res$error)
          expect_gt(res$stats$data[[1]], 0)
        } else {
          # erroring case
          expect_false(is.null(res$error))
        }
      })
    } else {
      # param set with no cases that will error (includes defaults)
      purrr::walk(results$results, function(res) {
        expect_s3_class(res, c("BenchmarkResult", "Serializable", "R6"))
        expect_null(res$error)
        expect_gt(res$stats$data[[1]], 0)
      })
    }
  })
})

unlink("benchconnect-state.json")
wipe_results()

do.call(Sys.setenv, old_env_vars)
