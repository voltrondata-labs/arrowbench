# Version of R6 with heritable static/class methods and attributes
#
# Elements in `static` can be called without instantiation, e.g. `Class$method()`.
# Functions are evaluated in the environment of `Class`, so you can refer to `self`
# (which is the class—not the instance—here) to create class methods.
#' @include util.R
R6.1Class <- function(..., static = NULL) {
  Class <- R6::R6Class(...)

  full_static <- modifyList(
    Class$get_inherit()$private_fields$static %||% list(),
    static %||% list()
  )
  full_static <- lapply(full_static, function(x) {
    if (!is.function(x)) return(x)
    environment(x) <- Class
    x
  })
  Class$set('private', 'static', full_static)

  for (name in names(Class$private_fields$static)) {
    Class[[name]] <- Class$private_fields$static[[name]]
  }

  Class
}


# Abstract class for a structure that can be written to JSON
#
# Private attributes with a `.` prefix will be serialized
Serializable <- R6.1Class(
  classname = "Serializable",

  public = list(
    write_json = function(path) {
      writeLines(self$json, path)
    }
  ),

  active = list(
    list = function() {
      private_names <- ls(private, all.names = TRUE)
      serializable_names <- private_names[startsWith(private_names, ".")]
      public_names <- sub('^\\.', '', serializable_names)

      json <- lapply(serializable_names, function(name) private[[name]])
      # recurse
      json <- lapply(json, function(element) {
        if (inherits(element, "Serializable")) {
          element <- element$json
        }
        element
      })
      names(json) <- public_names

      json
    },
    json = function() {
      jsonlite::toJSON(
        self$list,
        auto_unbox = TRUE,
        null = "null",
        POSIXt = "ISO8601",
        digits = 16L,
        pretty = TRUE
      )
    }
  ),

  private = list(
    get_or_set = function(variable, value) {
      if (!missing(value)) {
        private[[variable]] <- value
      }
      private[[variable]]
    }
  ),

  static = list(
    from_list = function(list) {
      do.call(self$new, list)
    },
    from_json = function(json) {
      self$from_list(jsonlite::fromJSON(paste(json, collapse = "\n"), simplifyMatrix = FALSE))
    },
    read_json = function(path) {
      self$from_json(paste(readLines(path), collapse = "\n"))
    },
    write_json = function(path) {
      writeLines(self$json, path)
    }
  )
)

#' @export
as.list.Serializable <- function(x, ...) x$list
#' @export
as.character.Serializable <- function(x, ...) x$json


BenchmarkResult <- R6.1Class(
  classname = "BenchmarkResult",
  inherit = Serializable,

  public = list(
    initialize = function(name,
                          result,
                          params,
                          tags = NULL,
                          info = NULL,
                          context = NULL,
                          github = NULL,
                          options = NULL,
                          output = NULL,
                          rscript = NULL) {
      self$name <- name
      self$result <- result
      self$params <- params
      self$tags <- tags
      self$info <- info
      self$context <- context
      self$github <- github
      self$options <- options
      self$output <- output
      self$rscript <- rscript
    },

    data.frame = function(row.names = NULL, optional = FALSE, packages = "arrow", ...) {
      x <- self$list

      pkgs <- x$params$packages
      x$params$packages <- NULL
      for (p in packages) {
        x$params[[paste0("version_", p)]] <- pkgs[p, "version"]
      }

      to_list_col <- lengths(x$params) != 1L
      x$params[to_list_col] <- lapply(x$params[to_list_col], list)

      out <- dplyr::bind_cols(
        iteration = seq_len(nrow(x$result)),
        x$result,
        dplyr::as_tibble(x$params)
      )
      out$output <- x$output

      # append metadata fields to dataframe as attributes
      metadata_elements <- c("name", "tags", "info", "context", "github", "options")
      for (element in metadata_elements) {
        if (element %in% names(x)) {
          attr(out, element) <- x[[element]]
        }
      }

      out
    }
  ),

  active = list(
    name = function(name) private$get_or_set(variable = ".name", value = name),
    result = function(result) private$get_or_set(variable = ".result", value = result),
    params = function(params) private$get_or_set(variable = ".params", value = params),
    tags = function(tags) private$get_or_set(variable = ".tags", value = tags),
    info = function(info) private$get_or_set(variable = ".info", value = info),
    context = function(context) private$get_or_set(variable = ".context", value = context),
    github = function(github) private$get_or_set(variable = ".github", value = github),
    options = function(options) private$get_or_set(variable = ".options", value = options),
    output = function(output) private$get_or_set(variable = ".output", value = output),
    rscript = function(rscript) private$get_or_set(variable = ".rscript", value = rscript),

    params_summary = function() {
      d <- self$params

      d$packages <- NULL

      to_list_col <- lengths(d) != 1
      d[to_list_col] <- lapply(d[to_list_col], list)

      d <- dplyr::as_tibble(d)
      d$did_error <- FALSE
      d
    }
  ),

  private = list(
    .name = NULL,
    .result = NULL,
    .params = NULL,
    .tags = NULL,
    .info = NULL,
    .context = NULL,
    .github = NULL,
    .options = NULL,
    .output = NULL,
    .rscript = NULL
  )
)


BenchmarkFailure <- R6.1Class(
  classname = "BenchmarkFailure",
  inherit = Serializable,

  public = list(
    initialize = function(error,
                          params) {
      self$error <- error
      self$params <- params
    }
  ),
  active = list(
    error = function(error) private$get_or_set(variable = ".error", value = error),
    params = function(params) private$get_or_set(variable = ".params", value = params),

    params_summary = function() {
      d <- self$params

      d$packages <- NULL
      to_list_col <- lengths(d) != 1
      d[to_list_col] <- lapply(d[to_list_col], list)

      d <- dplyr::as_tibble(d)
      d$did_error <- TRUE
      d
    }
  ),
  private = list(
    .error = NULL,
    .params = NULL
  )
)


BenchmarkResults <- R6.1Class(
  classname = "BenchmarkResults",
  inherit = Serializable,

  public = list(
    initialize = function(results) {
      self$results <- results
    },
    data.frame = function(row.names = NULL, optional = FALSE, ...) {
      x <- self$results
      valid <- map_lgl(x, ~inherits(.x, "BenchmarkResult"))  # failures will be BenchmarkFailure

      dplyr::bind_rows(lapply(x[valid], function(res) res$data.frame(...)))
    }
  ),

  active = list(
    results = function(results) private$get_or_set(variable = ".results", value = results),

    params_summary = function() {
      purrr::map_dfr(self$results, ~.x$params_summary)
    }
  ),

  private = list(
    .results = NULL
  )
)


#' Convert benchmark result object to a tidy data frame
#'
#' @param x a benchmark result object or list of them as returned by [run_one()] or [run_benchmark()]
#' @param row.names for generic consistency
#' @param optional for generic consistency
#' @param ... additional arguments passed on to methods for individual results.
#' `packages` is the only currently supported argument.
#'
#' @return A data.frame suitable for analysis in R
#' @export
as.data.frame.BenchmarkResults <- function(x, row.names = NULL, optional = FALSE, ...) {
  x$data.frame(row.names = row.names, optional = optional, ...)
}

#' @param packages Packages for which to extract versions
#' @rdname as.data.frame.BenchmarkResults
#' @export
as.data.frame.BenchmarkResult <- function(x, row.names = NULL, optional = FALSE, packages = "arrow", ...) {
  x$data.frame(row.names = row.names, optional = optional, packages = packages, ...)
}
