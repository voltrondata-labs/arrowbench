ensure_lib <- function(lib = NULL) {
  if (is.null(lib) || identical(lib, "latest")) {
    return(NULL)
  } else if (dir.exists(lib)) {
    return(lib)
  }

  lib_dir_path <- lib_dir(lib)
  if (dir.exists(lib_dir_path)) {
    return(lib_dir_path)
  }

  dir.create(lib_dir_path, recursive = TRUE)
  if (lib %in% names(known_lib_paths)) {
    # TODO
  } else if (grepl(".", lib, fixed = TRUE)) {
    # Install an arrow version from CRAN
    # Alternatively, install a nightly--just add nightly bintray/s3 to repos?
    remotes::install_version("arrow", lib, lib = lib_dir_path)
  } else {
    # git hash? build from source
    # TODO: use remotes package for github ref management
    # For mac, need to do what crossbow and arrow-r-nightly do to use autobrew and pin commit
  }
  lib_dir_path
}

# TODO: map lib dir names to commands that would install the things
known_lib_paths <- list()
# 1.0.1:
# cp ../dev/tasks/homebrew-formulae/autobrew/apache-arrow.rb tools/apache-arrow.rb
#   head "https://github.com/nealrichardson/arrow.git", :revision => "6c6c13c60600b747096538c044f22630a0dd654b"
#   patch do
#     url "https://github.com/apache/arrow/commit/ae60bad1c2e28bd67cdaeaa05f35096ae193e43a.patch"
#   end
# mkdir /Users/enpiar/Documents/ursa/ursa-qa/r/r_libs/1.0.1
# R CMD INSTALL -l !$ .


lib_dir <- function(..., local_dir = getOption("conbench.local_dir", getwd())) {
  file.path(local_dir, "r_libs", ...)
}