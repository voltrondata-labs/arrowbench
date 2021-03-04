# lib dirs are all inside the "r_libs" directory


#' @importFrom utils head tail install.packages installed.packages packageDescription
#' @importFrom remotes install_github
ensure_lib <- function(lib = NULL, test_packages = unlist(strsplit(packageDescription("arrowbench")[["Suggests"]], "[, \n]+"))) {
  # some packages need extra packages to do what we want
  # Listing them here is easier than installing with dependencies = TRUE
  if ("data.table" %in% test_packages) {
    test_packages <- c(test_packages, "R.utils")
  }
  # always install purrr + tibble for reasons:
  # when run_bm() is run inside of the benchmark it uses the packages found in
  # the r_lib library first then system/user packages. This is usually not a
  # problem, except the dependencies of arrowbench that aren't in r_lib can depend
  # on packages that are (and mis-match). For example: purrr suggests tibble
  # and the most recent version of tibble requires vctrs >= 0.3.2. *But* the
  # snapshot version of vctrs at that point is 0.3.0 causing a conflict.
  # TODO: maybe we should install all of the arrowbench dependencies?
  test_packages <- c(test_packages, "purrr", "tibble")

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

    # copy base, utils, and methods since they aren't installed from CRAN
    for (pkg in names(which(installed.packages()[ ,"Priority"] == "base", ))) {
      old_path <- system.file(package = pkg)
      file.copy(from = old_path, to = lib_dir_path, recursive = TRUE)
    }
  }

  with_pure_lib_path(lib_dir_path, {
    if (lib %in% names(arrow_version_to_date)) {
      # Install from a CRAN snapshot
      repo_url <- get_repo_url(lib)
      if (is_macos() && "data.table" %in% test_packages) {
        special_data_table_install(repo_url = repo_url)
        test_packages <- setdiff(test_packages, "data.table")
      }

      # make it install everything from this repo, install data.table separately (above) tho
      install.packages(test_packages, repos = repo_url)
    } else if (lib == "devel") {
      # using the canonical repo here for getting description files only
      cran_url <- "https://cloud.r-project.org/"
      # install the package from github

      install.packages("remotes")

      if (is_macos() && "data.table" %in% test_packages) {
        special_data_table_install(dev = TRUE, cran_url = cran_url)
        test_packages <- setdiff(test_packages, "data.table")
      }

      # make it install everything from this repo, install data.table separately (above) tho
      for (pkg in test_packages) {
        # TODO: error nicely if arrow fails to build because the libraries don't exist / are wrong?
        # fstcore fails(?) to build, so install that from CRAN proactively
        # TODO: break this out into a special_install function
        if ("fst" %in% test_packages) {
          install.packages("fstcore")
        }

        if (pkg == "arrow") {
          install_arrow_github()
        } else {
          remotes::install_dev(pkg, upgrade = "never", cran_url = cran_url)
        }
      }
    } else if (grepl("^remote-.*", lib)) {
      install.packages(c("remotes", setdiff(test_packages, "arrow")))
      # the form of the lib is remote-repo@ref
      args <- identify_repo_ref(lib)
      do.call(install_arrow_github, args)
    } else {
      # git hash? build from source
      # TODO: use remotes package for github ref management
      # For mac, need to do what crossbow and arrow-r-nightly do to use autobrew and pin commit
      stop("The lib_path is not a known value: ", lib, call. = FALSE)
    }
  })
  lib_dir_path
}

identify_repo_ref <- function(x) {
  repo <- gsub("^remote-(.*?)@(.*?)$", "\\1", x)
  ref <- gsub("^remote-(.*?)@(.*?)$", "\\2", x)

  list(repo = repo, ref = ref)
}

#' @importFrom withr with_makevars
install_arrow_github <- function(repo = "apache/arrow", ref = "HEAD", ...) {
  # Do we want or need to also allow this to install any github package? For
  # most it would only need to change the repo arg + expose the subdir argument
  env_vars <- list(
    # always use the tools/linuxlibs.R script for installing arrow lib
    FORCE_TOOLS_LIBS_SCRIPT = "true",
    # we want everything to be available
    LIBARROW_MINIMAL = "false",
    # for verbosity
    ARROW_R_DEV = "true"
  )

  # Newer versions of the macos sdk do not use /usr/include or the like to house
  # their headers, instead they are in a special root inside the developer SDK.
  # Setting the SDKROOT allowssome of arrow's dependencies find these.
  if (is_macos()) {
    env_vars <- append(env_vars, list(SDKROOT = system("xcrun --show-sdk-path", intern = TRUE)))
  }

  with_makevars(
    # make sure that we don't reference the system library locations
    list(CPPFLAGS = "", LDFLAGS = ""),
    with_envvar(
      env_vars,
      install_github(
        repo,
        ref = ref,
        subdir = "r",
        # build = FALSE is necessary so that the source package is not relocated
        # outside of the larger arrow repo when installing.
        build = FALSE,
        ...
      )
    )
  )
}

special_data_table_install <- function(repo_url = NULL, dev = FALSE, cran_url = NULL) {
  # installing data.table to allow for multi-cores is awkward on macOS
  withr::with_makevars(
    c(
      LLVM_LOC = "/usr/local/opt/llvm",
      CC = "$(LLVM_LOC)/bin/clang -fopenmp",
      CXX="$(LLVM_LOC)/bin/clang++ -fopenmp"
    ),
    install_func <-
      tryCatch({
        if (dev) {
          remotes::install_dev("data.table", type = "source", cran_url = cran_url)
        } else {
          install.packages("data.table",  repos = repo_url, type = "source")
        }
      },
      warning = function(w) {
        message(
          "You must install llvm and libomp before you can install data.table from ",
          "source. Try running `brew install llvm libomp` before re-running. ",
          "https://github.com/Rdatatable/data.table/wiki/Installation#openmp-enabled-compiler-for-mac ",
          "may have more information about how the dependencies needed to install ",
          "data.table with multicore awareness. The original error was:"
        )
        stop(w$message)
      }
      )
  )
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
  "2.0" = "2020-10-30",
  "3.0" = "2021-02-04"
)

# TODO: wrap their API,
# GET https://packagemanager.rstudio.com/__api__/status for distros
# GET https://packagemanager.rstudio.com/__api__/repos/1/transaction-dates?_sort=date&_order=asc for dates
rspm_ids <- c(
  "0.14" = 202, # actually 2019-08-21
  "0.15" = 230, # actually 2019-11-17
  "0.16" = 258, # actually 2020-02-26, since there is no snapshot for 2020-02-24
  "0.17" = 289, # actually 2020-05-31, since there is no snapshot for 2020-05-29
  "1.0" = 321,
  "2.0" = 357, # actually 2020-11-01
  "3.0" = 1194160
)

#' @importFrom distro distro
get_repo_url <- function(lib) {
  if (tolower(Sys.info()["sysname"]) == "linux") {
    # TODO: non-Ubuntu OSes need different handling
    # RSPM will send binaries as a fallback, so if the distro isn't perfect the worst that should happen is source installs.
    repo_url <-  paste0("https://packagemanager.rstudio.com/all/__linux__/",  distro()$codename, "/", rspm_ids[lib])
  } else {
    # macOS + windows
    repo_url <- paste0("https://mran.microsoft.com/snapshot/", arrow_version_to_date[lib])
  }
  repo_url
}

lib_dir <- function(..., local_dir = getOption("arrowbench.local_dir", getwd())) {
  r_version <- paste0(c(getRversion()$major, getRversion()$minor), collapse = ".")
  file.path(local_dir, "r_libs", paste0("R-", r_version),  ...)
}
