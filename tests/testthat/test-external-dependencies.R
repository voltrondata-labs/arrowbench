test_that("external_cli_available() works", {
  fake_uninstalled_cli <- basename(tempfile())
  expect_warning(
    expect_false(
      external_cli_available(fake_uninstalled_cli)
    ),
    regexp = paste(fake_uninstalled_cli, "not installed or on $PATH"),
    fixed = TRUE
  )

  expect_true(external_cli_available("which"))
})

test_that("pipx_available() works", {
  expect_equal(pipx_available(), processx::run("which", "pipx")$status == 0L)
})

test_that("benchconnect_available() works", {
  expect_equal(benchconnect_available(), processx::run("which", "benchconnect")$status == 0L)
})

test_that("datalogistik_available() works", {
  expect_equal(datalogistik_available(), processx::run("which", "datalogistik")$status == 0L)
})
