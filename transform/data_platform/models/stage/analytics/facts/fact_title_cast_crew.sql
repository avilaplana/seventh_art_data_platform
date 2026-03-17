SELECT 
    tpr.title_id,
    tpr.person_id,
    r.role_name,
    t.release_year,
    t.average_rating,
    t.number_of_votes
FROM {{ source('stage_canonical', 'title_person_role') }} tpr
JOIN {{ source('stage_canonical', 'title') }} t ON tpr.title_id = t.title_id
JOIN {{ source('stage_canonical', 'role') }} r ON tpr.role_id = r.role_id
