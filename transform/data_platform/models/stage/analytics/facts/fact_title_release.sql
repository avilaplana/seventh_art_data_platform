SELECT
    tl.title_id,
    tl.title as localized_title,
    tl.is_original_title,
    r.region_name,
    l.language_name    
FROM {{ source('stage_canonical', 'title_localized') }} tl
JOIN {{ source('stage_canonical', 'regions') }} r ON tl.region_id = r.region_id
JOIN {{ source('stage_canonical', 'languages') }} l ON tl.language_id = l.language_id
