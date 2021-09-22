library(dplyr)

test_that("can find the tables used", {
  test_query <- function(input_func) {
    input_func("fun_table") %>%
      collect()
  }
  expect_identical(tables_refed(test_query), "fun_table")

  # can also grab the ones that are from joins
  test_query <- function(input_func) {
    input_func("fun_table") %>%
      inner_join(input_func("less_fun_table"), by = "some_col")
      collect()
  }
  expect_identical(tables_refed(test_query), c("fun_table", "less_fun_table"))

  # a real query
  test_query <- function(input_func) {
    input_func("lineitem") %>%
      select(l_shipdate, l_returnflag, l_linestatus, l_quantity,
             l_extendedprice, l_discount, l_tax) %>%
      filter(l_shipdate <= as.Date("1998-09-02")) %>%
      select(l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax) %>%
      group_by(l_returnflag, l_linestatus) %>%
      summarise(
        sum_qty = sum(l_quantity),
        sum_base_price = sum(l_extendedprice),
        sum_disc_price = sum(l_extendedprice * (1 - l_discount)),
        sum_charge = sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
        avg_qty = mean(l_quantity),
        avg_price = mean(l_extendedprice),
        avg_disc = mean(l_discount),
        count_order = n()
      ) %>%
      arrange(l_returnflag, l_linestatus) %>%
      collect()
  }
  expect_identical(tables_refed(test_query), "lineitem")
})