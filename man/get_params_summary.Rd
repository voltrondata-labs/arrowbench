% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/benchmark.R
\name{get_params_summary}
\alias{get_params_summary}
\title{Extract the parameter summary as a data.frame}
\usage{
get_params_summary(run)
}
\arguments{
\item{run}{An instance of \code{BenchmarkResults} as returned by \code{run_benchmark}
or \code{BenchmarkResult} as returned by \code{run_one} and \code{run_bm}}
}
\value{
a tibble
}
\description{
Extract a data.frame that provides the parameters used in a run and the
error status
}
