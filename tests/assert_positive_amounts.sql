SELECT user_id
FROM {{ ref('stg_applications') }}
WHERE annual_income <= 0
   OR credit_amount <= 0
   OR loan_annuity <= 0
