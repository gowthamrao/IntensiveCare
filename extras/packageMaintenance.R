## baseUrl <- ""
# path <- ""

### Atlas cohorts

cohortIds <- c(15291, 15292, 15293, 15294, 15295, 15296, 15394)


# creates JSON objects/SQL files for cohorts of interest
cohortSetReferences <- list()
for (i in (1:length(cohortIds))) {#i = 1
  cohortId <- cohortIds[[i]]
  cohortName = ROhdsiWebApi::getCohortDefinitionName(baseUrl = baseUrl,
                                                     definitionId = cohortId,
                                                     formatName = TRUE) %>%
    stringr::str_replace(pattern = "- ", replacement = "")

  print(paste0(cohortId, ' ', cohortName))
  cohortSetReferences[[i]] <- tidyr::tibble(
    atlasId = cohortId,
    atlasName = cohortName,
    cohortId = cohortId,
    name = cohortName
  )

  ROhdsiWebApi::insertCohortDefinitionInPackage(
    definitionId = cohortId,
    name = cohortName,
    baseUrl = baseUrl,
    generateStats = TRUE
  )
}
cohortSetReference <-
  dplyr::bind_rows(cohortSetReferences) %>%
  tidyr::as_tibble()


dir.create(path = paste0(path, "inst/settings/"), showWarnings = FALSE)
readr::write_csv(
  x = cohortSetReference %>% dplyr::select(atlasId, atlasName, cohortId, name),
  path = paste0(path, "inst/settings/CohortsToCreate.csv")
)




#####################################################################
#####################################################################
#####################################################################
### Custom Cohorts - not in Atlas. No JSON OBJECTS
# as much as possible, we will leverage circe-be templates to build custom cohorts
# the approach is to build a custom query that mimics criteria query
# criteria query has the output person_id, start_date, end_date
# overlaps are allowed, duplicates are allowed. circe-be template queries will handle rest
# first occurrence, last occurrence, all occurrence handled by circe-be


#### downloading circe-Be queries from github to local
pathToTemplateSqlCircebe <- paste0(path, "inst/sql/sql_server/template/circeBe/")
downloadCirceFiles <- function(webUrl, sqlFileName, destFilePath) {
  dir.create(path = destFilePath, showWarnings = FALSE, recursive = TRUE)
  download.file(url = paste0(webUrl, sqlFileName), destfile = paste0(destFilePath, sqlFileName))
}

urlCirceBe <- 'https://raw.githubusercontent.com/OHDSI/circe-be/master/src/main/resources/resources/cohortdefinition/sql/'
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = "generateCohort.sql", destFilePath = pathToTemplateSqlCircebe)
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = "primaryEventsQuery.sql", destFilePath = pathToTemplateSqlCircebe)
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = "eraConstructor.sql", destFilePath = pathToTemplateSqlCircebe)
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = "eraConstructor.sql", destFilePath = pathToTemplateSqlCircebe)
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = "codesetQuery.sql", destFilePath = pathToTemplateSqlCircebe)
downloadCirceFiles(webUrl = urlCirceBe, sqlFileName = 'dateOffsetStrategy.sql', destFilePath = pathToTemplateSqlCircebe)


### intensive care custom cohorts - built using COST table
# these cohorts are built off of COST table in OMOP CDM v5.3
# they rely on revenue_code_concept_id being present in COST table
# the cost table query is '/inst/sql/sql_server'
# the custom SQL will behave like a criteria
## custom queries - this query mimics the output of criteria query (i.e. output: person_id, start_date, end_date)

# cost table custom query is based on selection of revenue_code_concept_id.
# the various conceptSetIds' for each of the custom query are identified by these Atlas ids
# first step is create a local copy of these concept sets - by resolving them to individual concept-ids.
# the individual concept-ids are then used in the custom sql template (criteria query)
conceptSetIds <- c(10735,10736,10737,10738,10834,10835,10836,10837,10838)
conceptSetReferences <- list()
for (i in (1:length(conceptSetIds))) { # i = 1
  conceptSetId <- conceptSetIds[[i]]

  conceptSetName <- ROhdsiWebApi::getConceptSetName(baseUrl = baseUrl, setId = conceptSetId, formatName = TRUE)
  conceptSetResolved <- ROhdsiWebApi::getConceptSetConceptIds(baseUrl = baseUrl, setId = conceptSetId)

  conceptSetReferences[[i]] <- tidyr::tibble(conceptSetId = conceptSetId,
                                    conceptSetName = conceptSetName,
                                    conceptIds = conceptSetResolved
                                    )
}
conceptSetReferences <- dplyr::bind_rows(conceptSetReferences)

dir.create(paste0(path, "/inst/concepts"), recursive = T, showWarnings = F)
readr::write_csv(x = conceptSetReferences, path = paste0(path, "/inst/concepts/conceptSetReferences.csv"))
saveRDS(object = conceptSetReferences, file = paste0(path, "/inst/concepts/conceptSetReferences.rds") )


# create sql templates per custom cohort
numberOfCustomCohorts <- conceptSetReferences %>%
  dplyr::select(conceptSetName) %>%
  unique() %>%
  dplyr::pull()


for (i in (1:length(numberOfCustomCohorts))) {#i = 1
  conceptSetName <- numberOfCustomCohorts[[i]]
  conceptSetReference <- conceptSetReferences %>% dplyr::filter(conceptSetName == !!conceptSetName)
  revenue_code_concept_id <- conceptSetReference %>%
    dplyr::select(conceptIds) %>%
    dplyr::pull() %>%
    paste0(collapse = ",")

  pathToTemplateSqlCustom <- paste0(path, "inst/sql/sql_server/template/custom")

  # custom sql with revenue code based query -- replaces criteria query and creates temp table: #criteriaQuery
  criteriqQueryRevenueCode <- SqlRender::readSql(paste0(pathToTemplateSqlCustom, "/criteriqQueryRevenueCode.sql"))
  criteriaQueries <- SqlRender::render(sql = criteriqQueryRevenueCode,
                         revenue_code_concept_id = revenue_code_concept_id)


  # primary event query, that uses #criteriaQuery
  primaryEventsQuery <- SqlRender::readSql(paste0(pathToTemplateSqlCircebe, "primaryEventsQuery.sql"))
  primaryEventsQuery <- SqlRender::render(sql = primaryEventsQuery,
                                          criteriaQueries = "select * from #criteriaQuery")


  # template sql for generating cohort.
  codesetQuery <- SqlRender::readSql(paste0(pathToTemplateSqlCircebe, 'codesetQuery.sql'))
  codesetQuery <- SqlRender::render(sql = codesetQuery,
                                    codesetInserts = '')
  dateOffsetStrategy <- SqlRender::readSql(paste0(pathToTemplateSqlCircebe, 'dateOffsetStrategy.sql'))
  dateOffsetStrategy <- SqlRender::render(sql = dateOffsetStrategy,
                                          dateField = 'end_date',
                                          offset = 0,
                                          eventTable = '#included_events')
  generateCohort <- SqlRender::readSql(paste0(pathToTemplateSqlCircebe, "generateCohort.sql"))
  generateCohort <- SqlRender::render(sql = generateCohort,
                                      primaryEventsQuery = primaryEventsQuery,
                                      codesetQuery = codesetQuery,
                                      strategy_ends_temp_tables = dateOffsetStrategy,
                                      generateStats = 0
                                      )

  # eraConstructor <- SqlRender::readSql(paste0(pathToTemplateSqlCircebe, 'eraConstructor.sql'))

  generateCohort <- paste0(criteriaQueries, generateCohort)

  ConnectionDetailMetaData <- ConnectionDetailsMetaData %>% dplyr::filter(databaseId == 'dod')
  scriptToRun <- SqlRender::render(sql = generateCohort,
                                   cdm_database_schema =  ConnectionDetailMetaData$cdmDatabaseSchema,
                                   EventSort = 'ASC',
                                   primaryEventLimit = '',
                                   primaryEventsFilter = "DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND
                                                          DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
                                                          ",
                                  QualifiedEventSort = 'ASC',
                                  additionalCriteriaQuery = '',
                                  QualifiedLimitFilter = '',
                                  inclusionCohortInserts = 'create table #inclusion_events (inclusion_rule_id bigint,	person_id bigint, event_id bigint);',
                                  IncludedEventSort = 'ASC',
                                  ruleTotal = 0,
                                 # strategy_ends_temp_tables = '',
                                  ResultLimitFilter = '',
                                 cohort_end_unions = 'SELECT event_id, person_id, end_date from #strategy_ends',
                                 eraconstructorpad = 0,
                                 finalCohortQuery =   "select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date
                                                        FROM #final_cohort CO
                                                      ",
                                 cohort_id_field_name = 'cohort_definition_id',
                                 strategy_ends_cleanup = 'DROP TABLE #strategy_ends;'

  )
  # scriptToRun <- SqlRender::translate(sql=scriptToRun, targetDialect = ConnectionDetailMetaData$dbms)

  SqlRender::writeSql(sql = scriptToRun, targetFile = paste0(path,'/inst/sql/sql_server/', conceptSetName, '.sql' ))
}






####### cohortToCreateWithSql will keep record of SQL scripts used to create cohorts

conceptSetReference <- conceptSetReferences %>% dplyr::select(conceptSetId, conceptSetName) %>% unique()
cohortsToCreateWithSql <- dplyr::bind_rows(

                      tidyr::tibble(category = 'Atlas',
                                    originalId = cohortSetReference$atlasId,
                                    name = cohortSetReference$atlasName,
                                    path = paste0("inst/sql/sql_server/",cohortSetReference$atlasName, '.sql' )
                                    )
                      ,
                      tidyr::tibble(category = 'Custom',
                                    originalId = conceptSetReference$conceptSetId,
                                    name = conceptSetReference$conceptSetName,
                                    path = paste0("inst/sql/sql_server/",conceptSetReference$conceptSetName, '.sql' )
                                    )

                      ) %>%
  unique() %>%
  dplyr::mutate(id = dplyr::row_number()) %>%
  dplyr::mutate(classification = dplyr::case_when(stringr::str_detect(string = name, pattern = 'Diagnosis') ~ 'diagnosis',
                                           stringr::str_detect(string = name, pattern = 'Intensive') ~ 'intensiveCare',
                                           stringr::str_detect(string = name, pattern = 'Sub-acute') ~ 'intensiveCare',
                                           TRUE ~ 'stay'

  ))




## render the sql with target_cohort_id
for (i in (1:nrow(cohortsToCreateWithSql))) {#i = 1
  cohortToCreateWithSql <- cohortsToCreateWithSql %>% dplyr::slice(i)
  sql <- SqlRender::readSql(sourceFile = paste0(path, cohortToCreateWithSql$path))
  sql <- SqlRender::render(sql = sql,
                           target_cohort_id = cohortToCreateWithSql$id)
  SqlRender::writeSql(sql = sql, targetFile = paste0(path,cohortToCreateWithSql$path ))
}





readr::write_csv(
  x = cohortsToCreateWithSql,
  path = paste0(path, "inst/sql/sql_server/cohortsToCreateWithSql.csv")
)


stayId <- cohortsToCreateWithSql %>% dplyr::filter(classification == 'stay') %>% dplyr::select(stayId = id, stay = name) %>% unique()
diagnosisId <- cohortsToCreateWithSql %>% dplyr::filter(classification == 'diagnosis') %>% dplyr::select(diagnosisId = id, diagnosis = name) %>% unique()
intensiveCareId <- cohortsToCreateWithSql %>% dplyr::filter(classification == 'intensiveCare') %>% dplyr::select(intensiveCareId = id, intensiveCare = name) %>% unique()

interSectCombos <- tidyr::crossing(stayId,intensiveCareId) %>%
                    dplyr::mutate(cohortId = ((stayId*100)+intensiveCareId),
                                  cohortName = paste0(stay, ' - ', intensiveCare)
                    )



pathToTemplateSqlPathway <- paste0(path, "inst/sql/sql_server/template/pathway/")
urlPathway <- 'https://raw.githubusercontent.com/OHDSI/WebAPI/master/src/main/resources/resources/pathway/'
downloadCirceFiles(webUrl = urlPathway, sqlFileName = "runPathwayAnalysis.sql", destFilePath = pathToTemplateSqlPathway)


