SELECT
    user_id,
    COUNT(*) AS record_count
FROM {{ ref('stg_applications') }}
GROUP BY user_id
HAVING COUNT(*) > 1
