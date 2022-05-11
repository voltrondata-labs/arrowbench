library(jsonlite)

wipe_results <- function() {
  unlink(test_path("results/"), recursive = TRUE)
  unlink(test_path("bm-scripts/"), recursive = TRUE)
}