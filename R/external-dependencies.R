external_cli_available <- function(cli) {
  res <- processx::run("which", cli, error_on_status = FALSE)

  if (res$status != 0L) {
    msg <- paste(cli, 'not installed or on $PATH.\n\n')
    if (cli == "pipx") {
      msg <- paste0(
        msg,
        glue::glue('It can be installed with `install_pipx()`\n\n'),
        'If already installed, ensure it is on $PATH, e.g. by running',
        '`pipx ensurepath` or adding `PATH="${PATH}:${HOME}/.local/bin"` to ~/.Renviron'
      )
    } else {
      msg <- paste0(
        msg,
        glue::glue('It can be installed with `install_pipx(); install_{cli}()`\n\n'),
        'If already installed with pipx, ensure it is on $PATH, e.g. by running',
        '`pipx ensurepath` or adding `PATH="${PATH}:${HOME}/.local/bin"` to ~/.Renviron'
      )
    }

    warning(warningCondition(msg, class = "notInstalledWarning"))
  }

  res$status == 0L
}

pipx_available <- function() {
  external_cli_available(cli = "pipx")
}

benchconnect_available <- function() {
  external_cli_available(cli = "benchconnect")
}

datalogistik_available <- function() {
  external_cli_available(cli = "datalogistik")
}


#' Install pipx
#'
#' Install [pipx](https://pypa.github.io/pipx/), a version of pip that installs
#' Python packages in isolated environments where they will always be available
#' regardless of which version of Python is presently on `$PATH`. Especially
#' useful for installing packages designed to be used via CLIs.
#'
#' @export
install_pipx <- function() {
  processx::run("sh", c("-c", "pip install pipx && pipx ensurepath"), echo_cmd = TRUE)
}


#' Install benchconnect
#'
#' Install [benchconnect](https://github.com/conbench/conbench/tree/main/benchconnect),
#' a utility for sending benchmark results to a Conbench server
#'
#' @export
install_benchconnect <- function() {
  stopifnot(pipx_available())

  url <- "benchconnect@git+https://github.com/conbench/conbench.git@main#subdirectory=benchconnect"

  if (suppressWarnings(benchconnect_available(), classes = "notInstalledWarning")) {
    if (interactive()) {
      ans <- readline("benchconnect already installed. Update? [Y/n]: ")
    } else {
      ans <- "y"
    }
    if (tolower(ans) %in% c("y", "")) {
      processx::run("pipx", c("install", "--include-deps", "--force", url), echo_cmd = TRUE)
    } else {
      invisible()
    }
  } else {
    processx::run("pipx", c("install", "--include-deps", url), echo_cmd = TRUE)
  }
}


#' Install datalogistik
#'
#' Install [datalogistik](https://github.com/conbench/datalogistik), a utility
#' for generating, downloading, and converting datasets for benchmarking.
#'
#' Only for interactive use.
#'
#' @export
install_datalogistik <- function() {
  # TODO: install pipx?
  stopifnot(pipx_available())

  ref <- Sys.getenv("DATALOGISTIK_BRANCH", unset = "main")
  url <- glue("git+https://github.com/conbench/datalogistik.git@{ref}")

  pipx_call <- c("install", "--pip-args=--extra-index-url https://pypi.fury.io/arrow-nightlies --prefer-binary")
  if (datalogistik_available()) {
    # default to yes (and also this will make it work in non-interactive sessions)
    ans <- readline("datalogistik already installed. Update? [Y/n]: ")
    if (tolower(ans) %in% c("y", "")) {
      # we need the extra args to depend on the development version of arrow
      return(processx::run("pipx", c(pipx_call, "--force", url), echo_cmd = TRUE))
    } else {
      return(invisible())
    }
  }

  processx::run("pipx", c(pipx_call, url), echo_cmd = TRUE)
}
