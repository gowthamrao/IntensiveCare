sqlCreateCodeSetTableTemplate <- "CREATE TABLE #Codesets (
codeset_id INT NOT NULL,
concept_id BIGINT NOT NULL
);
"


sqlInsertToCodeSetTemplate <- "
INSERT INTO #Codesets (
codeset_id,
concept_id
)
SELECT @codeset_id AS codeset_id,
c.concept_id
FROM (
  SELECT DISTINCT I.concept_id
  FROM (
    SELECT concept_id
    FROM @vocabulary_database_schema.CONCEPT
    WHERE concept_id IN (
      @concept_ids
    )
  ) I
) C;
"


sqlInsertConceptIdToCodeSet <- sqlCreateCodeSetTableTemplate
conceptSetsUsedInCostCohort <- readRDS(paste0(path, "/inst/concepts/concepts.rds") )

loopconceptSetsUsedInCostCohort <- conceptSetsUsedInCostCohort$studyId %>% unique()
for (i in (1:length(loopconceptSetsUsedInCostCohort))) {#i = 1
  conceptSet <- conceptSetsUsedInCostCohort %>% dplyr::filter(studyId == loopconceptSetsUsedInCostCohort[i])

  studyId <- conceptSet %>% dplyr::select(studyId) %>% unique() %>% dplyr::pull()
  conceptIds <- conceptSet %>% dplyr::select(conceptIds) %>% unique() %>% dplyr::pull() %>% stringr::str_c(collapse = ",")

  sqlInsertConceptIdToCodeSet <- paste(sqlInsertConceptIdToCodeSet,
                                       SqlRender::render(sqlInsertToCodeSetTemplate,
                                                         concept_ids = conceptIds,
                                                         codeset_id = studyId
                                       )
  )
}
