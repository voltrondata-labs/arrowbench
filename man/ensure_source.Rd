% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ensure-source.R
\name{ensure_source}
\alias{ensure_source}
\title{Make sure a data file exists}
\usage{
ensure_source(name, ...)
}
\arguments{
\item{name}{A known-source id, a file path, or a URL}

\item{...}{arguments to pass on to a custom locator}
}
\value{
A valid path to a source file. If a known source but not present,
it will be downloaded and possibly decompressed.
}
\description{
Make sure a data file exists
}
