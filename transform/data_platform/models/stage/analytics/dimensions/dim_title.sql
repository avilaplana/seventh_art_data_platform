SELECT
	t.title_id, 
	t.primary_title, 
	t.original_title, 
	tt.title_type_name, 
    t.release_year, 
	t.duration_minutes,
	t.is_adult, 
    t.average_rating,
    t.number_of_votes,
    CASE
        WHEN t.release_year IS NULL THEN NULL
        ELSE CONCAT(FLOOR(t.release_year / 10) * 10, 's')
    END AS decade,
    CASE
        WHEN t.average_rating IS NULL THEN 'no_rating'
        WHEN t.average_rating < 5 THEN '0-5'
        WHEN t.average_rating < 7 THEN '5-7'
        WHEN t.average_rating < 8 THEN '7-8'
        WHEN t.average_rating < 9 THEN '8-9'
        ELSE '9+'
    END AS rating_bucket
FROM {{ source('stage_canonical', 'title') }} t
JOIN {{ source('stage_canonical', 'title_type') }} tt ON t.title_type_id = tt.title_type_id
