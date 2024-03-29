% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/run.R
\name{run}
\alias{run}
\alias{run.BenchmarkDataFrame}
\title{Run an object}
\usage{
run(x, ...)

\method{run}{BenchmarkDataFrame}(x, ..., publish = FALSE, run_id = NULL, run_name = NULL, run_reason = NULL)
}
\arguments{
\item{x}{An S3 classed object to run}

\item{...}{Additional arguments passed through to methods. For
\code{run.BenchmarkDataFrame}, passed through to \code{\link[=get_default_parameters]{get_default_parameters()}} (when
parameters are not specified) and \code{\link[=run_benchmark]{run_benchmark()}}.}

\item{publish}{Flag for whether to publish results to a Conbench server. See
"Environment Variables" section for how to specify server details. Requires
the benchconnect CLI is installed; see \code{\link[=install_benchconnect]{install_benchconnect()}}.}

\item{run_id}{Unique ID for the run. If not specified, will be generated.}

\item{run_name}{Name for the run. If not specified, will use \verb{\{run_reason\}: \{commit hash\}}}

\item{run_reason}{Required. Low-cardinality reason for the run, e.g. "commit" or "test"}
}
\value{
A modified object containing run results. For \code{run.BenchmarkDataFrame},
a \code{results} list column is appended.
}
\description{
Run an object
}
\section{Environment Variables}{

\itemize{
\item \code{CONBENCH_URL}: Required. The URL of the Conbench server with no trailing
slash. For arrow, should be \verb{https://conbench.ursa.dev}.
\item \code{CONBENCH_EMAIL}: The email to use for Conbench login. Only required if the
server is private.
\item \code{CONBENCH_PASSWORD}: The password to use for Conbench login. Only required
if the server is private.
\item \code{CONBENCH_PROJECT_REPOSITORY}: The repository name (in the format
\code{org/repo}) or the URL (in the format \verb{https://github.com/org/repo}).
Defaults to \code{"https://github.com/apache/arrow"} if unset.
\item \code{CONBENCH_PROJECT_PR_NUMBER}: Recommended. The number of the GitHub pull
request that is running this benchmark, or \code{NULL} if it's a run on the
default branch
\item \code{CONBENCH_PROJECT_COMMIT}: The 40-character commit SHA of the repo being
benchmarked. If missing, will attempt to obtain it from
\code{arrow::arrow_info()$build_info$git_id}, though this may not be populated
depending on how Arrow was built.
\item \code{CONBENCH_MACHINE_INFO_NAME}: Will override detected machine host name sent
in \code{machine_info.name} when posting runs and results. Needed for cases where
the actual host name can vary, like CI and cloud runners.
}
}

