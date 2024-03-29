% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bm-tpc-h.R
\name{get_sql_query_func}
\alias{get_sql_query_func}
\title{Get a SQL query}
\usage{
get_sql_query_func(query_num)
}
\arguments{
\item{query_num}{the query number to fetch the result for}
}
\value{
a function that accepts an argument \code{con} which will run
\code{DBI::dbGetQuery()} against.
}
\description{
Produces a function that can be queried against any DBI backend (e.g. DuckDB)
}
\details{
The function that is returned takes the following arguments. The first two are
suppleid to match the signature of those in tpc_h_queries
\itemize{
\item \code{input_func} set to default \code{NULL}, will have no effect if supplied
\item \code{collect_func} set to default \code{NULL}, will have no effect if supplied
\item \code{con} a (DBI) connection to query against
}
}
\keyword{internal}
