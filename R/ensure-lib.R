# lib dirs are all inside the "r_libs" directory

test_packages <- unlist(strsplit(packageDescription("conbench")[["Suggests"]], "[, \n]+"))

ensure_lib <- function(lib = NULL) {
  if (is.null(lib) || identical(lib, "latest")) {
    return(NULL)
  } else if (dir.exists(lib)) {
    return(lib)
  }

  lib_dir_path <- lib_dir(lib)
  if (dir.exists(lib_dir_path)) {
    # Check that test packages are installed (i.e. that previous installation succeeded)
    test_packages <- setdiff(test_packages, dir(lib_dir_path))
    if (length(test_packages) == 0) {
      return(lib_dir_path)
    }
  } else {
    dir.create(lib_dir_path, recursive = TRUE)
  }

  if (lib %in% names(arrow_version_to_date)) {
    # Install from a CRAN snapshot
    # TODO: if linux use RSPM?
    repo_url <- paste0("https://mran.microsoft.com/snapshot/", arrow_version_to_date[lib])
    with_pure_lib_path(lib_dir_path,
      install.packages(test_packages, repos = repo_url)
    )
  } else {
    # git hash? build from source
    # TODO: use remotes package for github ref management
    # For mac, need to do what crossbow and arrow-r-nightly do to use autobrew and pin commit
  }
  lib_dir_path
}

with_pure_lib_path <- function(path, ...) {
  old <- .libPaths()
  on.exit(.libPaths(old))
  # Hack: assign this internal variable instead of using .libPaths because
  # .libPaths appends and we want to overwrite/exclude system/user libraries
  assign(".lib.loc", path, envir = environment(.libPaths))
  eval.parent(...)
}

# CRAN archive dates:
# [   ]	arrow_0.14.1.1.tar.gz	2019-08-09 00:40	103K
# [   ]	arrow_0.14.1.tar.gz	2019-08-05 18:10	103K
# [   ]	arrow_0.15.0.tar.gz	2019-10-07 21:00	144K
# [   ]	arrow_0.15.1.1.tar.gz	2019-11-05 23:00	144K
# [   ]	arrow_0.15.1.tar.gz	2019-11-04 23:20	144K
# [   ]	arrow_0.16.0.1.tar.gz	2020-02-10 23:40	208K
# [   ]	arrow_0.16.0.2.tar.gz	2020-02-14 13:20	211K
# [   ]	arrow_0.16.0.tar.gz	2020-02-09 13:20	208K
# [   ]	arrow_0.17.0.tar.gz	2020-04-21 20:10	237K
# [   ]	arrow_0.17.1.tar.gz	2020-05-19 22:30	237K
# [   ]	arrow_1.0.0.tar.gz	2020-07-25 06:30	268K
# [   ]	arrow_1.0.1.tar.gz	2020-08-28 14:20	268K
arrow_version_to_date <- c(
  # These are those archive dates (for the final patch release)
  # + 10 days to allow for delays in binary packaging
  "0.14" = "2019-08-19",
  "0.15" = "2019-11-15",
  "0.16" = "2020-02-24",
  "0.17" = "2020-05-29",
  "1.0" = "2020-09-07",
  "2.0" = "2020-10-30"
)

lib_dir <- function(..., local_dir = getOption("conbench.local_dir", getwd())) {
  file.path(local_dir, "r_libs", ...)
}
