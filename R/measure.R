#' Measure times and memory usage
#'
#' @param ... An expression to run
#' @inheritParams run_benchmark
#'
#' @return A tibble of timings and memory usage
#' @export
measure <- function(..., profiling = FALSE) {
  start_mem <- bench::bench_process_memory()
  gc_info <- with_gc_info({
    prof_file <- with_profiling(profiling, {
      timings <- bench::bench_time(eval.parent(...))
    })
  })
  end_mem <- bench::bench_process_memory()

  timings <- as.data.frame(as.list(timings))

  timings$start_mem_bytes <- as.numeric(start_mem["current"])
  timings$end_mem_bytes <- as.numeric(end_mem["current"])
  timings$max_mem_bytes <- as.numeric(end_mem["max"])
  timings$prof_file <- prof_file

  cbind(timings, gc_info)
}

with_profiling <- function(profiling_on, expr) {
  if (profiling_on) {
    prof_file <- basename(tempfile(fileext = ".prof"))
    utils::Rprof(filename = prof_file, memory.profiling = TRUE, gc.profiling = TRUE, line.profiling = TRUE)
    on.exit(utils::Rprof(NULL))
  } else {
    prof_file <- NULL
  }
  eval.parent(expr)
  prof_file
}

with_gc_info <- function(expr) {
  with_gcinfo <- "bench" %:::% "with_gcinfo"
  gc_output <- with_gcinfo(eval.parent(expr))
  # This will swallow errors, so check for error output and re-raise
  if (length(gc_output) > 0 && any(startsWith(gc_output, "Error")) ) {
    stop(paste(gc_output, collapse = "\n"), call. = FALSE)
  }
  parse_gc <- "bench" %:::% "parse_gc"
  gc <- parse_gc(gc_output)
  names(gc) <- paste0("gc_", names(gc))
  if (nrow(gc) == 0) {
    # Means there was no garbage collection, so let's fill this in with 0s
    gc[1, ] <- list(0L, 0L, 0L)
  }
  # Cat out any messages so that we don't swallow them.
  # TODO: filter out what has been parsed?
  cat(gc_output)
  gc
}

# work around checks looking for`:::`
`%:::%` = function(pkg, fun) get(fun, envir = asNamespace(pkg), inherits = FALSE)
