pipx_available <- function() {
  exit_code <- system("which pipx")
  exit_code == 0L
}

datalogistik_available <- function() {
  exit_code <- system("which datalogistik")

  if (exit_code != 0L) {
    warning(
      'datalogistik not installed or on $PATH.\n\n',
      'It can be installed interactively with `install_pipx(); install_datalogistik()`\n\n',
      'If already installed with pipx, ensure it is on $PATH, e.g. by running',
      '`pipx ensurepath` or adding `PATH="${PATH}:${HOME}/.local/bin"` to ~/.Renviron'
    )
  }

  exit_code == 0L
}


#' Install pipx
#'
#' Install [pipx](https://pypa.github.io/pipx/), a version of pip that installs
#' Python packages in isolated environments where they will always be available
#' regardless of which version of Python is presently on `$PATH`. Especially
#' useful for installing packages designed to be used via CLIs.
#'
#' Only for interactive use.
#'
#' @export
install_pipx <- function() {
  stopifnot(interactive())
  system('pip install pipx && pipx ensurepath', intern = TRUE)
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
  stopifnot(interactive(), pipx_available())

  if (datalogistik_available()) {
    ans <- readline("datalogistik already installed. Update? [y/n]: ")
    if (tolower(ans) == "y") {
      return(system("pipx reinstall datalogistik", intern = TRUE))
    } else {
      return(invisible())
    }
  }

  system('pipx install git+https://github.com/conbench/datalogistik.git', intern = TRUE)
}


# Call `datalogistik generate`
#
# @param A character vector of parameters with which to call datalogistik
#
# @return The returned metadata JSON parsed into a list
datalogistik_generate <- function(params) {
  stopifnot(datalogistik_available())

  command <- paste("datalogistik generate", paste(params, collapse = " "))
  metadata_json <- system(command, intern = TRUE)
  jsonlite::fromJSON(metadata_json)
}