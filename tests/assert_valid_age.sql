SELECT user_id
FROM {{ ref('stg_applications') }}
WHERE age_years < 18
   OR age_years > 100
