pipx_available <- function() {
  exit_code <- system("which pipx", ignore.stdout = TRUE, ignore.stderr = TRUE)
  exit_code == 0L
}

datalogistik_available <- function() {
  exit_code <- system("which datalogistik", ignore.stdout = TRUE, ignore.stderr = TRUE)

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
  # TODO: install pipx?
  stopifnot(pipx_available())

  ref <- Sys.getenv("DATALOGISTIK_BRANCH", unset = "main")
  url <- glue("git+https://github.com/conbench/datalogistik.git@{ref}")

  if (datalogistik_available()) {
    # default to yes (and also this will make it work in non-interactive sessions)
    ans <- readline("datalogistik already installed. Update? [Y/n]: ")
    if (tolower(ans) %in% c("y", "")) {
      return(system(glue("pipx install --force {url}"), intern = TRUE))
    } else {
      return(invisible())
    }
  }

  system(glue("pipx install {url}"), intern = TRUE)
}


#' Call `datalogistik get`
#'
#' @param dataset the dataset to get
#' @param format the format of the dataset to be in (default: parquet)
#' @param compression the compression of the dataset to use (default: uncompressed)
#' @param scale_factor the scale_factor of the dataset (for datasets that use it)
#'
#' @return The returned metadata JSON parsed into a list
#' @importFrom processx run
datalogistik_get <- function(dataset, format = NULL, compression = NULL, scale_factor = NULL) {
  stopifnot(datalogistik_available())

  # This might be needed for certificates to work, somehow we should do this inside of datalogistik too.
  # Sys.setenv(SSL_CERT_FILE = "~/envs/datalogistik/lib/python3.9/site-packages/certifi/cacert.pem")

  args <- c(
    "get",
    glue("--dataset={dataset}"),
    # these will return character(0) if they are NULL
    glue("--format={format}"),
    glue("--compression={compression}"),
    glue("--scale-factor={scale_factor}")
  )
  # TODO: if this errors, then actually show the output?
  dl_out <- run("datalogistik", args, error_on_status = FALSE)

  if (dl_out$status > 0) {
    stop("There was an error with datalogistik", dl_out$stderr)
  }

  jsonlite::fromJSON(dl_out$stdout)
}