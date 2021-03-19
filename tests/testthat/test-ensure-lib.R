test_that("lib_dir()", {
  expect_identical(
    lib_dir("foo"),
    file.path(getwd(), "r_libs", paste0("R-", paste0(c(getRversion()$major, getRversion()$minor), collapse = ".")), "foo")
  )

  expect_identical(
    lib_dir("remote-user/arrow@branch/with/slashes"),
    file.path(getwd(), "r_libs", paste0("R-", paste0(c(getRversion()$major, getRversion()$minor), collapse = ".")), "remote-user_arrow@branch_with_slashes")
  )
})

test_that("identify_repo_ref()", {
  expect_identical(
    identify_repo_ref("remote-name/repo@ref"),
    list(repo = "name/repo", ref = "ref")
  )
})