library(jsonlite)

wipe_results <- function() unlink(test_path("results/"), recursive = TRUE)
