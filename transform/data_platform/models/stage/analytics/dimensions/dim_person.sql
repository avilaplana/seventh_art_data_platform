SELECT
	person_id,
	name, 
	birth_year, 
	death_year
FROM {{ source('stage_canonical', 'person') }}