% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/run.R
\name{run_bm}
\alias{run_bm}
\title{Execute a benchmark run}
\usage{
run_bm(
  bm,
  ...,
  n_iter = 1,
  batch_id = NULL,
  profiling = FALSE,
  global_params = list(),
  run_id = NULL,
  run_name = NULL,
  run_reason = NULL
)
}
\arguments{
\item{bm}{\code{\link[=Benchmark]{Benchmark()}} object}

\item{...}{parameters passed to \code{bm$setup()} or global parameters; see the
"Parameterizing benchmarks" section of \code{\link[=Benchmark]{Benchmark()}}}

\item{n_iter}{Integer number of iterations to replicate each benchmark}

\item{batch_id}{a length 1 character vector to identify the batch}

\item{profiling}{Logical: collect prof info? If \code{TRUE}, the result data will
contain a \code{prof_file} field, which you can read in with
\code{profvis::profvis(prof_input = file)}. Default is \code{FALSE}}

\item{global_params}{the global parameters that have been set}

\item{run_id}{Unique ID for the run}

\item{run_name}{Name for the run}

\item{run_reason}{Low-cardinality reason for the run, e.g. "commit" or "test"}
}
\description{
This is the function that gets called in the script that \code{\link[=run_one]{run_one()}} prepares.
You may call this function interactively, but you won't get the isolation
in a fresh R process that \code{run_one()} provides.
}
