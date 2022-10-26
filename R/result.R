#' Version of R6 with heritable static/class methods and attributes
#'
#' Elements in `static` can be called without instantiation, e.g. `Class$method()`.
#' Functions are evaluated in the environment of `Class`, so you can refer to `self`
#' (which is the class—not the instance—here) to create class methods.
#'
#' @param ... Passed through to [R6::R6Class]
#' @param static A named list of static/class functions/values to turn into
#' methods/attributes. Note there is currently no differentiation between static
#' and class methods at the moment; static methods are simply class methods that
#' do not access `self`, though it will exist in their evaluation environment.
#' This arrangement can be changed in the future if reason exists.
#'
#' @section Why this exists:
#'
#' Sometimes we want static/class methods/attributes that can be accessed from
#' the class (e.g. `MyR6Class$my_static_method()`) instead of an instance of
#' that class (e.g. `MyR6Class$new(...)$my_normal_method()`). As individual
#' classes are environments, these can be added after the fact like so:
#'
#' ``` r
#' MyR6Class <- R6Class(...)
#' MyR6Class$my_static_method <- function(x) ...
#' ```
#'
#' But the problem with the above is it's not heritable; if you make a class that
#' inherits from `MyR6Class`, it will not have `$my_static_method()` unless you
#' manually re-add it.
#'
#' This class structure abstracts the pattern, so when you create a new class, it
#' checks if the parent contains anything in `private$static`, and copies over any
#' methods/attributes there, less any overwritten in the new class.
#'
#' @section How static/class methods/attributes may be useful:
#'
#' There are lots of reasons you may want static/class methods/attributes, but
#' the immediate use-case here is to create alternate methods for instantiating
#' a class besides `$new()`/`$initialize()`. For instance, if a class can be
#' represented as JSON, it's quite helpful to have a `$from_json()` method that
#' can recreate an instance from a JSON blob.
#'
#' You could have a separate special reader function that returns an instance,
#' but especially as classes multiply this solution becomes difficult to
#' maintain.
#'
#' @keywords internal
#' @include util.R
R6Point1Class <- function(..., static = NULL) {
  Class <- R6::R6Class(...)
  Class$parent_env <- parent.frame()

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
# This is an abstract class that is not useful on its own, but if taken as a
# parent class provides methods for serializing and deserializing to/from
# JSON-like representations (JSON strings, JSON files, lists representing JSON).
#
# Specifically:
# - `$json`: Active binding that returns a JSON string
# - `$list`: Active binding that returns a list
# - `$write_json()`: Method that writes a JSON representation to a file
# - `$from_json()`: Class method that creates an instance from a JSON string
# - `$from_list()`: Class method that creates an instance from a list
# - `$read_json()`: Class method that creates an instance from a JSON file
#
# Note that attributes stored in `private$to_serialize` will be serialized;
# other attributes will not, including public attributes not duplicated in
# `private$to_serialize`. This scheme can be changed if there is reason.
#
Serializable <- R6Point1Class(
  classname = "Serializable",

  public = list(
    write_json = function(path) {
      writeLines(self$json, path)
    }
  ),

  active = list(
    list = function() {
      modifyList(self$list_serializable, private$not_to_serialize)
    },
    list_serializable = function() {
      lapply(private$to_serialize, function(element) {
        # recurse
        if (inherits(element, "Serializable")) {
          element <- element$list
        }
        element
      })
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
    to_serialize = list(),
    not_to_serialize = list(),

    get_or_set_serializable = function(variable, value) {
      if (!missing(value)) {
        private$to_serialize[[variable]] <- value
      }
      private$to_serialize[[variable]]
    },

    get_or_set_not_to_serialize = function(variable, value) {
      if (!missing(value)) {
        private$not_to_serialize[[variable]] <- value
      }
      private$not_to_serialize[[variable]]
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
    }
  )
)

#' @export
as.list.Serializable <- function(x, ...) x$list
#' @export
as.character.Serializable <- function(x, ...) x$json


# A class for the results of succesfully running a benchmark
#
# Because this class inherits from `Serializable`, it can be written to and
# instantiated from JSON forms.
#
# An instance can be passed to `as.data.frame()` and `get_params_summary()`, the
# returns of which are simply what would be returned from the `$to_dataframe()`
# method and the `$params_summary` active binding.
#
# All attributes are active bindings so that validation can be run when they are
# set, whether during or after instantiation.
BenchmarkResult <- R6Point1Class(
  classname = "BenchmarkResult",
  inherit = Serializable,

  public = list(
    initialize = function(name,
                          result = NULL,
                          params = NULL,
                          tags = NULL,
                          info = NULL,
                          context = NULL,
                          github = NULL,
                          options = NULL,
                          output = NULL,
                          rscript = NULL,
                          error = NULL) {
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
      self$error <- error
    },

    to_dataframe = function(row.names = NULL, optional = FALSE, packages = "arrow", ...) {
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
    },

    to_publishable_json = function() {
      res_list <- self$list_serializable

      if (!is.null(res_list$result)) {
        res_list[["stats"]] <- list(
          data = list(res_list$result$real),
          units = "s",
          iterations = length(res_list$result$real),
          times = list(),
          times_unit = "s"
        )
        res_list$result <- NULL
      }

      res_list$tags$name <- res_list$name
      res_list$name <- NULL

      jsonlite::toJSON(res_list, auto_unbox = TRUE)
    }
  ),

  active = list(
    name = function(name) private$get_or_set_serializable(variable = "name", value = name),
    result = function(result) private$get_or_set_serializable(variable = "result", value = result),
    params = function(params) private$get_or_set_not_to_serialize(variable = "params", value = params),
    tags = function(tags) private$get_or_set_serializable(variable = "tags", value = tags),
    info = function(info) private$get_or_set_serializable(variable = "info", value = info),
    context = function(context) private$get_or_set_serializable(variable = "context", value = context),
    github = function(github) private$get_or_set_serializable(variable = "github", value = github),
    options = function(options) private$get_or_set_not_to_serialize(variable = "options", value = options),
    output = function(output) private$get_or_set_not_to_serialize(variable = "output", value = output),
    rscript = function(rscript) private$get_or_set_not_to_serialize(variable = "rscript", value = rscript),
    error = function(error) private$get_or_set_serializable(variable = "error", value = error),

    params_summary = function() {
      d <- self$params

      d$packages <- NULL

      to_list_col <- lengths(d) != 1
      d[to_list_col] <- lapply(d[to_list_col], list)

      d <- dplyr::as_tibble(d)
      d$did_error <- FALSE
      d
    }
  )
)


# A class for representing an unsuccessful benchmark run
#
# Can be serialized to JSON, though as of writing is not anywhere programmatically.
BenchmarkFailure <- R6Point1Class(
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
    error = function(error) private$get_or_set_serializable(variable = "error", value = error),
    params = function(params) private$get_or_set_serializable(variable = "params", value = params),

    params_summary = function() {
      d <- self$params

      d$packages <- NULL
      to_list_col <- lengths(d) != 1
      d[to_list_col] <- lapply(d[to_list_col], list)

      d <- dplyr::as_tibble(d)
      d$did_error <- TRUE
      d
    }
  )
)


# A class for holding a set of benchmark results
#
# This class is primarily a list of `BenchmarkResult` instances, one for each
# combination of arguments for the benchmark's parameters. The list is accessible
# via the `$results` active binding.
#
# An instance can be passed to `as.data.frame()` and `get_params_summary()`, the
# returns of which are simply what would be returned from the `$to_dataframe()`
# method and the `$params_summary` active binding.
#
# Can be serialized to JSON, though as of writing is not anywhere programmatically.
#
BenchmarkResults <- R6Point1Class(
  classname = "BenchmarkResults",
  inherit = Serializable,

  public = list(
    initialize = function(results) {
      self$results <- results
    },
    to_dataframe = function(row.names = NULL, optional = FALSE, ...) {
      x <- self$results
      valid <- purrr::map_lgl(x, ~!is.null(.x$result))

      dplyr::bind_rows(lapply(x[valid], function(res) res$to_dataframe(...)))
    }
  ),

  active = list(
    results = function(results) private$get_or_set_serializable(variable = "results", value = results),

    params_summary = function() {
      purrr::map_dfr(self$results, ~.x$params_summary)
    }
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
  x$to_dataframe(row.names = row.names, optional = optional, ...)
}

#' @param packages Packages for which to extract versions
#' @rdname as.data.frame.BenchmarkResults
#' @export
as.data.frame.BenchmarkResult <- function(x, row.names = NULL, optional = FALSE, packages = "arrow", ...) {
  x$to_dataframe(row.names = row.names, optional = optional, packages = packages, ...)
}
