WITH stg_credit_card AS (
    -- This pulls from your Staging file
    SELECT * FROM {{ ref('stg_credit_card_balance') }}
)

SELECT
    *,

    -- Using CASE to prevent division by zero
    CASE
        WHEN AMT_CREDIT_LIMIT_ACTUAL > 0
        THEN AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL
        ELSE NULL
    END AS utilization_ratio,

    CASE
        WHEN AMT_INST_MIN_REGULARITY > 0 AND AMT_PAYMENT_CURRENT > 0
        THEN AMT_PAYMENT_CURRENT / AMT_INST_MIN_REGULARITY
        ELSE NULL
    END AS payment_to_min_ratio,

    CASE
        WHEN AMT_CREDIT_LIMIT_ACTUAL > 0 AND AMT_DRAWINGS_CURRENT > 0
        THEN AMT_DRAWINGS_CURRENT / AMT_CREDIT_LIMIT_ACTUAL
        ELSE NULL
    END AS drawing_to_limit_ratio,

    -- Metadata
    CURRENT_TIMESTAMP() AS stg_processed_at
    
FROM stg_credit_card