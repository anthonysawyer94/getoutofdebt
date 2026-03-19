WITH stg_bureau AS (
    SELECT * FROM {{ ref('stg_bureau') }}
)

SELECT
    *,

    -- Individual Loan Ratios (The "Metrics")
    -- Using CASE to prevent division by zero
    CASE 
        WHEN total_credit_amount > 0 
        THEN current_debt / total_credit_amount 
        ELSE NULL 
    END AS debt_to_credit_ratio,

    CASE 
        WHEN total_credit_amount > 0 
        THEN overdue_amount / total_credit_amount 
        ELSE NULL 
    END AS overdue_to_credit_ratio,

    -- High-Risk Flags (Optional but great for risk models)
    CASE WHEN days_overdue > 0 THEN 1 ELSE 0 END AS is_currently_overdue,

    -- Metadata
    CURRENT_TIMESTAMP() AS int_bureau_calculated_at

FROM stg_bureau