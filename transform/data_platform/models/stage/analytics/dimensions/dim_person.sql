SELECT
	person_id,
	name,
	birth_year,
	death_year,
	death_year IS NULL AS is_alive
FROM {{ source('stage_canonical', 'person') }}
