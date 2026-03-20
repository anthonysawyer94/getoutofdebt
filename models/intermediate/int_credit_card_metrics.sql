{{ config(materialized = 'view') }}

WITH stg_credit_card AS (
    SELECT * FROM {{ ref('stg_credit_card_balance') }}
)

SELECT
    *,

    -- Using CASE to prevent division by zero
    CASE
        WHEN credit_limit > 0
        THEN balance / credit_limit
        ELSE NULL
    END AS utilization_ratio,

    CASE
        WHEN min_installment > 0 AND current_payment > 0
        THEN current_payment / min_installment
        ELSE NULL
    END AS payment_to_min_ratio,

    CASE
        WHEN credit_limit > 0 AND total_drawings > 0
        THEN total_drawings / credit_limit
        ELSE NULL
    END AS drawing_to_limit_ratio,

    -- Metadata
    CURRENT_TIMESTAMP() AS int_credit_card_calculated_at
    
FROM stg_credit_card