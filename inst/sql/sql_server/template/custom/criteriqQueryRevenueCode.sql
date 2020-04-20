WITH cost
AS (
	SELECT cost_event_id,
		cost_domain_id
	FROM @cdm_database_schema.cost c
	WHERE revenue_code_concept_id IN (
			@revenue_code_concept_id
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
	JOIN @cdm_database_schema.procedure_occurrence AS p ON cost.cost_event_id = p.procedure_occurrence_id
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
	JOIN @cdm_database_schema.condition_occurrence c ON cost.cost_event_id = c.condition_occurrence_id
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
	JOIN @cdm_database_schema.visit_occurrence v ON cost.cost_event_id = v.visit_occurrence_id
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
	JOIN @cdm_database_schema.observation AS o ON cost.cost_event_id = o.observation_id
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
	JOIN @cdm_database_schema.device_exposure AS d ON cost.cost_event_id = d.device_exposure_id
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
	JOIN @cdm_database_schema.measurement AS m ON cost.cost_event_id = m.measurement_id
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

