sourceKeys <-
  c(
    "IBM_MDCR",
    "IBM_MDCD",
    "IBM_CCAE",
    "OPTUM_PANTHER",
    "OPTUM_EXTENDED_DOD",
    "IQVIA_GERMANY_DA",
    "IQVIA_FRANCE_DA",
    "IBM_MDCR",
    "IBM_MDCD",
    "IBM_CCAE",
    "CPRD",
    "IQVIA_AUSTRALIA_EMR"
  )


##################

path <- rstudioapi::getActiveProject()

library(magrittr)
projectPath <- rstudioapi::getActiveProject()
cohortDatabaseSchema <- Sys.getenv("cohortDatabaseSchema")
cohortTable <- 'cohort'

connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = Sys.getenv("dbms"),
    user = NULL,
    password = NULL,
    port = Sys.getenv("port"),
    server = Sys.getenv("server")
  )
oracleTempSchema <- NULL
cdmDataSource <-
  ROhdsiWebApi::getCdmSources(baseUrl = Sys.getenv("baseUrl")) %>% tidyr::as_tibble()




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

sqlProcedureDuringCohortPeriod <-
  "SELECT CAST(procedure_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
c.concept_name AS covariate_name,
procedure_concept_id AS concept_id,
COUNT(*) AS sum_value,
COUNT(*) * 1.0 / stat.total_cnt * 1.0 AS average_value
FROM (
SELECT DISTINCT procedure_concept_id,
cohort.subject_id,
cohort.cohort_start_date
FROM @target_database_schema.@target_cohort_table cohort
INNER JOIN @cdm_database_schema.procedure_occurrence ON cohort.subject_id = procedure_occurrence.person_id
WHERE procedure_date <= dateadd(d, 0, cohort.cohort_start_date)
AND procedure_date <= dateadd(d, 0, cohort.cohort_end_date)
AND procedure_concept_id != 0
AND cohort.cohort_definition_id = @cohort_id
) procedure_entries
JOIN @vocabulary_database_schema.concept c ON procedure_entries.procedure_concept_id = c.concept_id
CROSS JOIN (
SELECT COUNT(*) total_cnt
FROM @target_database_schema.@target_cohort_table
WHERE cohort_definition_id = @cohort_id
) stat
GROUP BY procedure_concept_id,
c.concept_name,
stat.total_cnt"


sqlObservationDuringCohortPeriod <-
  "SELECT cast(observation_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
c.concept_name AS covariate_name,
observation_concept_id AS concept_id,
count(*) AS sum_value,
count(*) * 1.0 / stat.total_cnt * 1.0 AS average_value
FROM (
SELECT DISTINCT observation_concept_id,
cohort.subject_id,
cohort.cohort_start_date
FROM @target_database_schema.@target_cohort_table cohort
INNER JOIN @cdm_database_schema.observation ON cohort.subject_id = observation.person_id
WHERE observation_date >= dateadd(d, 0, cohort.cohort_start_date)
AND observation_date <= dateadd(d, 0, cohort.cohort_end_date)
AND observation_concept_id != 0
AND cohort.cohort_definition_id = @cohort_id
) observation_entries
JOIN @vocabulary_database_schema.concept c ON observation_entries.observation_concept_id = c.concept_id
CROSS JOIN (
SELECT count(*) total_cnt
FROM @target_database_schema.@target_cohort_table
WHERE cohort_definition_id = @cohort_id
) stat
GROUP BY observation_concept_id,
c.concept_name,
stat.total_cnt
"


sqlConditionDuringCohortPeriod <-
  "SELECT cast(condition_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
c.concept_name AS covariate_name,
condition_concept_id AS concept_id,
count(*) AS sum_value,
count(*) * 1.0 / stat.total_cnt * 1.0 AS average_value
FROM (
SELECT DISTINCT condition_concept_id,
cohort.subject_id,
cohort.cohort_start_date
FROM @target_database_schema.@target_cohort_table cohort
INNER JOIN @cdm_database_schema.condition_occurrence ON cohort.subject_id = condition_occurrence.person_id
WHERE condition_start_date >= dateadd(d, 0, cohort.cohort_start_date)
AND condition_start_date <= dateadd(d, 0, cohort.cohort_end_date)
AND condition_concept_id != 0
AND cohort.cohort_definition_id = @cohort_id
) condition_entries
JOIN @vocabulary_database_schema.concept c ON condition_entries.condition_concept_id = c.concept_id
CROSS JOIN (
SELECT count(*) total_cnt
FROM @target_database_schema.@target_cohort_table
WHERE cohort_definition_id = @cohort_id
) stat
GROUP BY condition_concept_id,
c.concept_name,
stat.total_cnt
"

sqlDrugEraDuringCohortPeriod <- "SELECT CAST(drug_concept_id AS BIGINT) * 1000 + @analysis_id AS covariate_id,
	c.concept_name AS covariate_name,
  drug_concept_id AS concept_id,
  COUNT(*) AS sum_value,
  COUNT(*) * 1.0 / stat.total_cnt * 1.0 AS average_value
FROM (
SELECT DISTINCT drug_concept_id,
cohort.subject_id,
cohort.cohort_start_date
FROM @target_database_schema.@target_cohort_table cohort
INNER JOIN @cdm_database_schema.drug_era ON cohort.subject_id = drug_era.person_id
WHERE drug_era_start_date <= dateadd(d, 0, cohort.cohort_start_date)
AND drug_era_start_date <= dateadd(d, 0, cohort.cohort_end_date)
AND drug_concept_id != 0
AND cohort.cohort_definition_id = @cohort_id
) drug_entries
JOIN @vocabulary_database_schema.concept c ON drug_entries.drug_concept_id = c.concept_id
CROSS JOIN (
SELECT COUNT(*) total_cnt
FROM @target_database_schema.@target_cohort_table
WHERE cohort_definition_id = @cohort_id
) stat
GROUP BY drug_concept_id,
c.concept_name,
stat.total_cnt
"


finalResult <- list()
k <- 0
for (j in 1:nrow(sqlFilesToRun)) {
  #j = 1
  for (i in 1:length(sourceKeys)) {
    #i = 1
    connection <- DatabaseConnector::connect(connectionDetails)
    k <- k + 1
    sourceKey <- sourceKeys[[i]]
    cdmDatabaseSchema <- cdmDataSource %>%
      dplyr::filter(sourceKey == !!sourceKey) %>%
      dplyr::select(cdmDatabaseSchema) %>%
      dplyr::pull() %>%
      toupper()
    vocabDatabaseSchema <- cdmDataSource %>%
      dplyr::filter(sourceKey == !!sourceKey) %>%
      dplyr::select(vocabDatabaseSchema) %>%
      dplyr::pull() %>% toupper()

    sqlFileToRun <- sqlFilesToRun %>% dplyr::slice(j)
    sqlFileName <- sqlFileToRun$sqlFileName
    cohortId <- sqlFileToRun$id



    sqlFile <- paste0(path, "/inst/sql/sql_server/", sqlFileName)
    sql <- SqlRender::readSql(sqlFile)
    sql <- SqlRender::render(
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      target_cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema
    )
    sql <-
      SqlRender::translate(sql = sql, targetDialect = connection@dbms)


    print(paste0("Running for ", sourceKey, " cohort from ", sqlFileName))

    DatabaseConnector::executeSql(connection = connection,
                                  sql = sql)

    # procedure results
    sql <- SqlRender::render(
      sql = sqlProcedureDuringCohortPeriod,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      analysis_id = 1
    )
    sql <-
      SqlRender::translate(sql = sql, targetDialect = connection@dbms)
    procedureResults <-
      DatabaseConnector::querySql(connection = connection,
                                  sql = sql) %>%
      tidyr::as_tibble() %>%
      dplyr::mutate(
        SOURCE_KEY = sourceKey,
        CDM_DATABASE_SCHEMA = cdmDatabaseSchema,
        DOMAIN = 'Procedure',
        COHORT_ID = cohortId,
        SQL_FILE_NAME = sqlFileToRun$sqlFileName
      )


    # observation results
    sql <- SqlRender::render(
      sql = sqlObservationDuringCohortPeriod,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      analysis_id = 2
    )
    sql <-
      SqlRender::translate(sql = sql, targetDialect = connection@dbms)
    observationResults <-
      DatabaseConnector::querySql(connection = connection,
                                  sql = sql) %>%
      tidyr::as_tibble() %>%
      dplyr::mutate(
        SOURCE_KEY = sourceKey,
        CDM_DATABASE_SCHEMA = cdmDatabaseSchema,
        DOMAIN = 'Observation',
        COHORT_ID = cohortId,
        SQL_FILE_NAME = sqlFileToRun$sqlFileName
      )



    # condition results
    sql <- SqlRender::render(
      sql = sqlConditionDuringCohortPeriod,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      analysis_id = 3
    )
    sql <-
      SqlRender::translate(sql = sql, targetDialect = connection@dbms)
    conditionResults <-
      DatabaseConnector::querySql(connection = connection,
                                  sql = sql) %>%
      tidyr::as_tibble() %>%
      dplyr::mutate(
        SOURCE_KEY = sourceKey,
        CDM_DATABASE_SCHEMA = cdmDatabaseSchema,
        DOMAIN = 'Condition',
        cohortId = cohortId,
        SQL_FILE_NAME = sqlFileToRun$sqlFileName
      )


    # drug era results
    sql <- SqlRender::render(
      sql = sqlDrugEraDuringCohortPeriod,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_id = cohortId,
      target_cohort_table = cohortTable,
      target_database_schema = cohortDatabaseSchema,
      vocabulary_database_schema = vocabDatabaseSchema,
      analysis_id = 4
    )
    sql <-
      SqlRender::translate(sql = sql, targetDialect = connection@dbms)
    conditionResults <-
      DatabaseConnector::querySql(connection = connection,
                                  sql = sql) %>%
      tidyr::as_tibble() %>%
      dplyr::mutate(
        SOURCE_KEY = sourceKey,
        CDM_DATABASE_SCHEMA = cdmDatabaseSchema,
        DOMAIN = 'DrugEra',
        COHORT_ID = cohortId,
        SQL_FILE_NAME = sqlFileToRun$sqlFileName
      )

    result <- dplyr::bind_rows(procedureResults,observationResults,conditionResults)
    DatabaseConnector::disconnect(connection)
    finalResult[[k]] <- result
  }
}

output <- dplyr::bind_rows(finalResult)

localServerConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = 'sqlite',
                                                                           server = paste0(path, '/results/output', '.sqlite'),
                                                                           port = NULL)
localServerConnection = DatabaseConnector::connect(connectionDetails = localServerConnectionDetails)
DatabaseConnector::insertTable(connection = localServerConnection,
                               tableName = output,
                               data = data,
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = FALSE,
                               useMppBulkLoad = FALSE)

output <- saveRDS(file = paste0(path, '/results/output.rds'))
output <- readRDS(file = paste0(path, '/results/output.rds'))
