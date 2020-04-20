# make sure the latest version of this package is installed in R-studio.
library(magrittr)
# connection details
source('D:/ignore/ignoreThisFile.R') # source of ConnectionDetailsMetaData, baseUrl
oracleTempSchema <- NULL

cdmDataSources <-
  ROhdsiWebApi::getCdmSources(baseUrl = baseUrl) %>%
  tidyr::as_tibble()

path <- paste0(rstudioapi::getActiveProject(), "/")
projectCode <- stringr::str_replace(string = basename(path), pattern= '_', replacement = '')
# projectCode <- 'IntensiveCare'
packageName <- projectCode
cohortTable <- paste0('cohort_', projectCode)
ffLocation <- paste0('D:/ff/', projectCode)
dir.create(path = ffLocation, showWarnings = FALSE, recursive = TRUE)


############ instantiating SQL cohorts ###############
for (i in (1:nrow(ConnectionDetailsMetaData))) {
  ConnectionDetailMetaData <- ConnectionDetailsMetaData %>% dplyr::slice(i)
  server <- ConnectionDetailMetaData$server
  port <- ConnectionDetailMetaData$port
  baseUrl <- ConnectionDetailMetaData$baseUrl
  dbms <- ConnectionDetailMetaData$dbms
  cdmDatabaseSchema <- ConnectionDetailMetaData$cdmDatabaseSchema
  cohortDatabaseSchema <- ConnectionDetailMetaData$cohortDatabaseSchema
  vocabDatabaseSchema <- ConnectionDetailMetaData$vocabularyDatabaseSchema
  sourceName <- cdmDataSources  %>%
    dplyr::filter(sourceKey %in% c(ConnectionDetailMetaData$sourceKey %>% unique())) %>%
    dplyr::select(sourceName) %>%
    dplyr::pull()

  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = dbms,
      user = Sys.getenv("user"),
      password = Sys.getenv("password"),
      port = port,
      server = server
    )

  ####################
  #### check for the presence of cohort table. If not exists, create it.
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  tablesInCohortDatabaseSchema <-
    tidyr::tibble(tableName = DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)) %>%
    dplyr::filter(tableName == !!toupper(cohortTable)) %>%
    dplyr::pull() %>%
    unique()
  if (isFALSE(length(tablesInCohortDatabaseSchema) > 0)) {
    CohortDiagnostics::createCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      createInclusionStatsTables = FALSE
    )
  }
  DatabaseConnector::disconnect(connection)
  ####################

  cohortsToCreateWithSql <- readr::read_csv(file =  paste0(path, "inst/sql/sql_server/cohortsToCreateWithSql.csv"), col_types = readr::cols())

  for (j in (1:nrow(cohortsToCreateWithSql))) {# j = 1
    cohortToCreateWithSql <- cohortsToCreateWithSql %>% dplyr::slice(j)
    sql <- SqlRender::readSql(sourceFile = paste0(path, cohortToCreateWithSql$path))
    sql <- SqlRender::render(
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      results_database_schema = cohortDatabaseSchema
    )
    sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)

    print(paste0("Instantiating cohort using sql: ",cohortToCreateWithSql$name," in ",ConnectionDetailMetaData$databaseName))

    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    DatabaseConnector::executeSql(connection = connection,sql = sql)
    DatabaseConnector::disconnect(connection)
  }

}


############ instantiating derived cohorts (intersect/union/difference) ###############




  # sqlFilesToRun <- tidyr::tibble(files = list.files(path = paste0(path, 'inst/sql/sql_server/'), full.names = TRUE)) %>%
  #                   dplyr::mutate(sqlFileName = basename(files),
  #                                 path = dirname(files)) %>%
  #                   dplyr::filter(stringr::str_detect(string = sqlFileName, pattern = ".sql"))




























atlasIds <-
  readr::read_csv(file = paste0(path, '/inst/settings/atlasIds.csv'), col_types = readr::cols())




# domains to perform domain anlaysis.
domainsToExtractFeatures <-
  readr::read_csv(system.file("csv", "PrespecAnalyses.csv", package = "FeatureExtraction")) %>%
  dplyr::filter(sqlFileName == 'DomainConcept.sql') %>%
  dplyr::select(domainId, domainTable, domainConceptId, domainStartDate) %>%
  unique()
sqlDomain <-
  SqlRender::readSql(sourceFile = paste0(path, '/inst/sql/sql_server/sqlDomainFeatures.sql'))


# sQL to get cohort summary information - counts/days
sqlCohortCount <- "SELECT cohort_definition_id,
count(*) records,
count(distinct subject_id) subjects,
sum(DATEDIFF(d,cohort_start_date, cohort_end_date) + 1) days
FROM @target_database_schema.@target_cohort_table
where cohort_definition_id = @cohort_id
group by cohort_definition_id"




# connection details
source('D:/ignore/ignoreThisFile.R') # source of ConnectionDetailsMetaData, baseUrl
# connectionDetailsMetaData is dataFrame with all character fields for
#   databaseId server          cohortDatabaseSc~  port sourceKey   databaseName          databaseDescription               baseUrl   cdmDatabaseSche~ vocabularyDatab~ dbms


source(paste0(path, '/extras/ConceptSetCode.R')) # source of conceptSetsUsedInCostCohort, sqlInsertConceptIdToCodeSet
# source(paste0(path, '/extras/sql.R')) # source of sqlFilesToRun



# final output
features <- list()
incidentRateAnlaysis <- list()
cohortCounts <- list()


for (i in (1:nrow(ConnectionDetailsMetaData))) {
  #i = 2
  ConnectionDetailMetaData <-
    ConnectionDetailsMetaData %>% dplyr::slice(i)
  server <- ConnectionDetailMetaData$server
  port <- ConnectionDetailMetaData$port
  baseUrl <- ConnectionDetailMetaData$baseUrl
  dbms <- ConnectionDetailMetaData$dbms
  cdmDatabaseSchema <- ConnectionDetailMetaData$cdmDatabaseSchema
  cohortDatabaseSchema <-
    ConnectionDetailMetaData$cohortDatabaseSchema
  vocabDatabaseSchema <-
    ConnectionDetailMetaData$vocabularyDatabaseSchema
  sourceName <- cdmDataSources  %>%
    dplyr::filter(sourceKey %in% c(ConnectionDetailMetaData$sourceKey %>% unique())) %>%
    dplyr::select(sourceName) %>%
    dplyr::pull()

  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = dbms,
      user = Sys.getenv("user"),
      password = Sys.getenv("password"),
      port = port,
      server = server
    )

  ####################
  #### check for the presence of cohort table. If not exists, create it.
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  tablesInCohortDatabaseSchema <-
    tidyr::tibble(tableName = DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)) %>%
    dplyr::filter(tableName == !!toupper(cohortTable)) %>%
    dplyr::pull() %>%
    unique()
  if (isFALSE(length(tablesInCohortDatabaseSchema) > 0)) {
    CohortDiagnostics::createCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      createInclusionStatsTables = FALSE
    )
  }
  DatabaseConnector::disconnect(connection)
  ####################



  ############# create custom cohorts using SQL ################
  customCohortsToCreate <- atlasIds %>%
    dplyr::filter(type == 'custom cohort')

  for (j in (1:nrow(customCohortsToCreate))) {
    #j = 1

    cohortName <-
      customCohortsToCreate %>% dplyr::slice(j) %>% dplyr::pull(studyName)
    cohortId <-
      customCohortsToCreate %>% dplyr::slice(j) %>% dplyr::pull(studyId)

    conceptIds <- conceptSetsUsedInCostCohort %>%
      dplyr::filter(studyName == !!cohortName) %>%
      dplyr::select(studyId, conceptIds) %>%
      unique() %>%
      dplyr::pull() %>%
      stringr::str_c(collapse = ",")

    costCohortSql <- paste0(
      sqlInsertConceptIdToCodeSet,
      SqlRender::readSql(
        paste0(path, "/inst/sql/sql_server/", 'sqlCostCohort.sql')
      ),
      SqlRender::readSql(paste0(
        path, "/inst/sql/sql_server/", 'sqlMagic.sql'
      )),
      SqlRender::readSql(
        paste0(
          path,
          "/inst/sql/sql_server/",
          'sqlInsertCohortAllRows.sql'
        )
      )
    )

    costCohortSql <- SqlRender::render(
      sql = costCohortSql,
      cdm_database_schema = cdmDatabaseSchema,
      target_cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      codeset_id = cohortId
    )
    costCohortSql <-
      SqlRender::translate(sql = costCohortSql, targetDialect = connectionDetails$dbms)


    print(
      paste0(
        "Data Source: ",
        sourceName,
        ". Cohort: ",
        cohortName,
        ". CohortId:",
        cohortId
      )
    )

    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)
    DatabaseConnector::executeSql(connection = connection,
                                  sql = costCohortSql)
    DatabaseConnector::disconnect(connection)
  }


  ################################
  # cohorts from atlas created cohorts - specified as SQL
  ## cohort definitions created using atlas cohort definition and exported as JSON/SQL
  atlasCohortsToCreate <- atlasIds %>%
    dplyr::filter(type == 'atlas cohort')

  for (k in (1:nrow(atlasCohortsToCreate))) {
    # k = 1
    atlasCohortToCreate <-
      atlasIds %>% dplyr::filter(type == 'atlas cohort' &
                                   codeType == 'cohortDefinition') %>% dplyr::slice(k)

    cohortName <- atlasCohortToCreate %>% dplyr::pull(studyName)
    cohortId <- atlasCohortToCreate %>% dplyr::pull(studyId)

    cohortSql <-
      SqlRender::readSql(paste0(
        path,
        '/inst/sql/sql_server/',
        atlasCohortToCreate$studyName,
        '.sql'
      ))
    cohortSql <- SqlRender::render(
      sql = cohortSql,
      cdm_database_schema = cdmDatabaseSchema,
      target_cohort_id = atlasCohortToCreate$studyId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema
    )
    cohortSql <-
      SqlRender::translate(sql = cohortSql, targetDialect = connectionDetails$dbms)
    print(
      paste0(
        "Data Source: ",
        sourceName,
        ". Cohort: ",
        cohortName,
        ". CohortId: ",
        cohortId
      )
    )

    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)
    DatabaseConnector::executeSql(connection = connection,
                                  sql = cohortSql)
    DatabaseConnector::disconnect(connection)
  }


  ######################### pending #################
  # cohort using intersect/union or intersect
  # + sqlMagic





  ###############
  # extract features for cohorts

  features <- list()

  cohortDomainFeatures <- list()
  cohortIncidentRateAnlaysis <- list()
  cohortCountResults <- list()
  for (m in (1:nrow(atlasIds))) {
    #m = 1
    atlasId <- atlasIds %>% dplyr::slice(m)
    cohortId = atlasId %>% dplyr::select(studyId) %>% dplyr::pull()
    cohortName = atlasId %>% dplyr::select(studyName) %>% dplyr::pull()

    domainFeatures <- list()
    for (n in (1:nrow(domainsToExtractFeatures))) {
      #n = 1
      domainsToExtractFeature <-
        domainsToExtractFeatures %>% dplyr::slice(n)
      featureSql <- sqlDomain
      featureSql <- SqlRender::render(
        sql = featureSql,
        analysis_id = m,
        cdm_database_schema = cdmDatabaseSchema,
        target_cohort_table = cohortTable,
        target_database_schema = cohortDatabaseSchema,
        vocabulary_database_schema = vocabDatabaseSchema,
        cohort_id = cohortId,
        domain_concept_id = domainsToExtractFeature$domainConceptId,
        domain_table = domainsToExtractFeature$domainTable,
        domain_start_date = domainsToExtractFeature$domainStartDate
      )
      featureSql <-
        SqlRender::translate(sql = featureSql, targetDialect = connectionDetails$dbms)


      print(
        paste0(
          "Extracting features: Data Source: ",
          sourceName,
          ". Cohort: ",
          cohortName,
          ". CohortId: ",
          cohortId,
          '. Domain table: ',
          domainsToExtractFeature$domainTable
        )
      )

      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)
      temp <-
        DatabaseConnector::querySql(connection = connection,
                                    sql = featureSql) %>%
        dplyr::mutate(
          domain_table = domainsToExtractFeature$domainTable,
          cohort_name = cohortName,
          source_name = sourceName
        )
      names(temp) <-
        SqlRender::snakeCaseToCamelCase(names(temp))
      domainFeatures[[n]] <- temp
      DatabaseConnector::disconnect(connection)
      print('     complete')
    }
    cohortDomainFeatures[[m]] <- dplyr::bind_rows(domainFeatures)

    cohortIncidentRateAnlaysis[[m]] <-
      CohortDiagnostics::getIncidenceRate(
        connectionDetails = connectionDetails,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cdmDatabaseSchema = cdmDatabaseSchema,
        firstOccurrenceOnly = TRUE,
        washoutPeriod = 365,
        cohortId = cohortId
      )

    # cohort count
    sqlCohortCountRender <- SqlRender::render(
      sql = sqlCohortCount,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      cohort_id = cohortId
    )
    sqlCohortCountRender <-
      SqlRender::translate(sql = sqlCohortCountRender, targetDialect = connectionDetails$dbms)
    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)
    temp <- DatabaseConnector::querySql(connection = connection,
                                        sql = sqlCohortCountRender) %>%
      dplyr::mutate(cohort_name = cohortName,
                    source_name = sourceName)
    names(temp) <- SqlRender::snakeCaseToCamelCase(names(temp))
    cohortCountResults[[m]] <- temp %>% tidyr::tibble()

  }
  features[[i]] <- dplyr::bind_rows(cohortDomainFeatures)
  incidentRateAnlaysis[[i]] <-
    dplyr::bind_rows(cohortIncidentRateAnlaysis)  %>%
    dplyr::mutate(cohort_name = cohortName,
                  source_name = sourceName)
  cohortCounts[[i]] <- dplyr::bind_rows(cohortCountResults)

}

features <- dplyr::bind_rows(features) %>%
  dplyr::select(
    sourceName,
    cohortDefinitionId,
    domainTable,
    cohortName,
    conceptId,
    conceptName = covariateName,
    sumValue,
    averageValue
  ) %>%
  tidyr::tibble()

incidentRateAnlaysis <- dplyr::bind_rows(incidentRateAnlaysis) %>%
  tidyr::tibble()

cohortCounts <- dplyr::bind_rows(cohortCounts) %>%
  tidyr::tibble()

saveRDS(object = features,
        file = paste0(path, '/results/features.rds'))
saveRDS(object = incidentRateAnlaysis,
        file = paste0(path, '/results/incidentRateAnlaysis.rds'))
saveRDS(object = cohortCounts,
        file = paste0(path, '/results/cohortCounts.rds'))




#
# cohort_results2 <- cohort_results
#
# cohort_results3 <- cohort_results2 %>%
#   dplyr::mutate(cohortCount = sumValue/averageValue,
#                 cohort = stringr::str_replace(string = sqlFileName, pattern = '.sql', replacement = ''),
#                 tab = paste0(sourceKey,'-',domain)
#                 ) %>%
#   dplyr::mutate(cohort = stringr::str_replace(string = cohort, pattern = 'intensiveCareR', replacement = 'R')) %>%
#   # dplyr::mutate(cohort = paste0(cohort, " (n = ", scales::comma(cohortCount*1000), ")")) %>%
#   # dplyr::mutate(averageValue = scales::comma(averageValue*100, accuracy = 0.1)) %>%
#   dplyr::filter(domain %in% c('Procedure', 'Observation')) %>%
#   dplyr::select(tab, cohort, conceptId, covariateName, averageValue)
#
# cohort_results4 <- cohort_results3 %>%
#   dplyr::group_by(tab) %>%
#   tidyr::pivot_wider(names_from = cohort,
#                      values_from = averageValue,
#                      values_fill = list(averageValue = 0))
#
# cohort_results5 <- split(cohort_results4, cohort_results4$tab)
#
# openxlsx::write.xlsx(x = cohort_results5, file = paste0(path, "/excelOutput.xlsx"))
