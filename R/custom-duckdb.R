
ensure_custom_duckdb <- function() {
  # We need to check if the installed duckdb has the tpch extension built. If it
  # does not, we will build it (with the appropriate envvars to build with tpch)

  # We check if duckdb has the tpch extension. This is done in a call to `system`
  # so that we don't load the duckdb namespace/dll before installing it. In my
  # testing even pkgload::unload() couldn't fully unload duckdb.
  lines <- c(
    "con <- DBI::dbConnect(duckdb::duckdb())",
    "DBI::dbGetQuery(con, 'select scale_factor, query_nr from tpch_answers() LIMIT 1;')",
    "DBI::dbDisconnect(con, shutdown = TRUE)"
  )

  # If there's an error, duckdb_cant_tpch will be 1
  duckdb_cant_tpch <- suppressWarnings(system(
    paste(find_r(), "--no-save -s"),
    ignore.stdout = TRUE,
    ignore.stderr = TRUE,
    input = lines
  ))

  if (duckdb_cant_tpch >= 1) {
    message("Installing duckdb with the ability to generate tpc-h datasets")
    install_custom_duckdb()

    # Warn that the session will have to be reset
    if ("duckdb" %in% loadedNamespaces()) {
      warning(
        "************************************************************************\n",
        "The duckdb package was loaded prior to checking if it had all of the\n",
        "necesary features. If you run into errors, please restart your R session\n",
        "and try again to ensure that the newly installed duckdb is being used.\n",
        "************************************************************************\n",
        call. = FALSE
      )
    }
  }
}

install_custom_duckdb <- function() {
  # build = false so that the duckdb cpp source is available when the R package
  # is compiling itself
  withr::with_envvar(
    list(DUCKDB_R_EXTENSIONS = "tpch"),
    # build duckdb with tpch enabled
    remotes::install_github("duckdb/duckdb/tools/rpkg", build = FALSE)
  )
}
