WITH cost
AS (
	SELECT cost_event_id,
		cost_domain_id
	FROM cdm.cost c
	WHERE revenue_code_concept_id IN (
			38003109
			)
	),
event_dates
AS (
	SELECT person_id,
		p.visit_occurrence_id,
		p.procedure_occurrence_id as event_id,
		'Procedure' as domain_id,
		procedure_date AS start_date,
		procedure_date AS end_date,
		procedure_date as sort_date,
		procedure_concept_id as target_concept_id
	FROM cost
	JOIN cdm.procedure_occurrence AS p ON cost.cost_event_id = p.procedure_occurrence_id
	WHERE cost_domain_id = 'Procedure'

	UNION ALL

	SELECT person_id,
		c.visit_occurrence_id,
		c.condition_occurrence_id as event_id,
		'Condition' as domain_id,
		condition_start_date AS start_date,
		COALESCE(c.condition_end_date, DATEADD(day,1,c.condition_start_date)) AS end_date,
		condition_start_date as sort_date,
		condition_concept_id as target_concept_id
	FROM cost
	JOIN cdm.condition_occurrence c ON cost.cost_event_id = c.condition_occurrence_id
	WHERE cost_domain_id = 'Condition'

	UNION ALL

	SELECT person_id,
		v.visit_occurrence_id,
		v.visit_occurrence_id as event_id,
		'Visit' as domain_id,
		visit_start_date AS start_date,
		COALESCE(v.visit_end_date, DATEADD(day,1,v.visit_start_date)) AS end_date,
		visit_start_date as sort_date,
		visit_concept_id as target_concept_id
	FROM cost
	JOIN cdm.visit_occurrence v ON cost.cost_event_id = v.visit_occurrence_id
	WHERE cost_domain_id = 'Visit'

	UNION ALL

	SELECT person_id,
		o.visit_occurrence_id,
		o.observation_id as event_id,
		'Observation' as domain_id,
		observation_date AS start_date,
		observation_date AS end_date,
		observation_date as sort_date,
		observation_concept_id as target_concept_id
	FROM cost
	JOIN cdm.observation AS o ON cost.cost_event_id = o.observation_id
	WHERE cost_domain_id = 'Observation'

	UNION ALL

	SELECT person_id,
		d.visit_occurrence_id,
		d.device_exposure_id as event_id,
		'Device' as domain_id,
		device_exposure_start_date AS start_date,
		COALESCE(d.device_exposure_end_date, DATEADD(day,1,d.device_exposure_start_date)) AS end_date,
		device_exposure_start_date as sort_date,
		device_concept_id as target_concept_id
	FROM cost
	JOIN cdm.device_exposure AS d ON cost.cost_event_id = d.device_exposure_id
	WHERE cost_domain_id = 'Device'

	UNION ALL

	SELECT person_id,
		m.visit_occurrence_id,
		m.measurement_id as event_id,
		'Measurement' as domain_id,
		measurement_date AS start_date,
		measurement_date AS end_date,
		measurement_date as sort_date,
		measurement_concept_id as target_concept_id
	FROM cost
	JOIN cdm.measurement AS m ON cost.cost_event_id = m.measurement_id
	WHERE cost_domain_id = 'Measurement'
	)
SELECT distinct person_id,
	visit_occurrence_id,
	event_id,
	domain_id,
	start_date,
	end_date,
	sort_date,
	target_concept_id
INTO #criteriaQuery
FROM event_dates;

CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;




with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  select * from #criteriaQuery
  ) E
	JOIN cdm.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND
                                                          DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
                                                          
) P

-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,	person_id bigint, event_id bigint);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),0)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;

-- date offset strategy

select event_id, person_id, 
  case when DATEADD(day,0,end_date) > start_date then DATEADD(day,0,end_date) else start_date end as end_date
INTO #strategy_ends
from #included_events;


-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  SELECT event_id, person_id, end_date from #strategy_ends
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = 15;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select 15 as cohort_definition_id, person_id, start_date, end_date
                                                        FROM #final_cohort CO
                                                      
;



DROP TABLE #strategy_ends;

TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
