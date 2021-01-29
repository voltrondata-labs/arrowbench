test_that("lib_dir()", {
  expect_identical(
    lib_dir("foo"),
    file.path(getwd(), "r_libs", paste0("R-", paste0(c(getRversion()$major, getRversion()$minor), collapse = ".")), "foo")
  )
})


