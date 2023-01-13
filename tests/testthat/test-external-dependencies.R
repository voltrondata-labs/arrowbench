test_that("pipx_available() works", {
  expect_equal(pipx_available(), system("which pipx") == 0L)
})

test_that("benchconnect_available() works", {
  expect_equal(benchconnect_available(), system("which benchconnect") == 0L)
})

test_that("datalogistik_available() works", {
  expect_equal(datalogistik_available(), system("which datalogistik") == 0L)
})
