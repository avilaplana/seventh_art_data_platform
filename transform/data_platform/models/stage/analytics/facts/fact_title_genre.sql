WITH title_genre AS (
    SELECT 
        t.title_id,    
        g.genre_name
    FROM {{ source('stage_canonical', 'title') }} t
    JOIN {{ source('stage_canonical', 'genre_title') }} tg ON t.title_id = tg.title_id
    JOIN {{ source('stage_canonical', 'genre') }} g ON tg.genre_id = g.genre_id
), title_genre_aggregation AS (
        SELECT 
            title_id,
            ARRAY_AGG(genre_name) AS genre_names,
            COUNT(genre_name) AS genre_count
        FROM title_genre
        GROUP BY title_id
)
SELECT
    tga.title_id,
    tga.genre_names,
    tga.genre_count,
    t.release_year,
    t.average_rating,
    t.number_of_votes
FROM title_genre_aggregation tga
JOIN {{ source('stage_canonical', 'title') }} t ON tga.title_id = t.title_id        
