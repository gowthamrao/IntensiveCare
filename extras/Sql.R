sqlFilePath <- paste0(path, '/inst/sql/sql_server/')
sqlFilesToRun <-
  list.files(
    path = sqlFilePath,
    pattern = ".sql$",
    all.files = FALSE,
    full.names = FALSE,
    recursive = FALSE,
    ignore.case = FALSE,
    include.dirs = FALSE
  )
sqlFilesToRun <- tidyr::tibble(sqlFileName = sqlFilesToRun) %>%
  dplyr::mutate(id = dplyr::row_number())
