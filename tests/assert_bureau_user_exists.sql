SELECT
    b.user_id
FROM {{ ref('stg_bureau') }} b
LEFT JOIN {{ ref('stg_applications') }} a ON b.user_id = a.user_id
WHERE a.user_id IS NULL
