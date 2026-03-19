{{ config(materialized = 'view') }}

WITH bureau_staging AS (
    SELECT * FROM {{ ref('stg_bureau') }}
)

SELECT
    user_id,

    COUNT(*) AS total_bureau_credits,
    COUNT(DISTINCT bureau_credit_id) AS unique_bureau_credits,

    -- Active vs closed
    SUM(CASE WHEN credit_status = 'Active' THEN 1 ELSE 0 END) AS active_credits,
    SUM(CASE WHEN credit_status = 'Closed' THEN 1 ELSE 0 END) AS closed_credits,

    -- Credit amounts
    SUM(total_credit_amount) AS total_bureau_credit_amount,
    AVG(total_credit_amount) AS avg_bureau_credit_amount,
    MAX(total_credit_amount) AS max_bureau_credit_amount,
    MIN(total_credit_amount) AS min_bureau_credit_amount,

    -- Debt
    SUM(current_debt) AS total_bureau_debt,
    AVG(current_debt) AS avg_bureau_debt,

    -- Overdue
    SUM(overdue_amount) AS total_overdue_amount,
    MAX(max_overdue_amount) AS max_overdue_ever,
    AVG(days_overdue) AS avg_days_overdue,

    -- Credit duration
    AVG(days_since_credit) AS avg_days_since_credit,
    MIN(days_since_credit) AS earliest_credit_days,
    MAX(days_since_credit) AS latest_credit_days,

    -- Debt ratios
    CASE
        WHEN SUM(total_credit_amount) > 0
        THEN SUM(current_debt) / SUM(total_credit_amount)
        ELSE NULL
    END AS overall_debt_ratio,

    -- Credit types
    COUNT(DISTINCT credit_type) AS num_credit_types,

    -- Annuity
    AVG(annuity) AS avg_annuity,
    SUM(annuity) AS total_annuity,

    -- Prolongation
    SUM(num_times_prolonged) AS total_prolongations,

    CURRENT_TIMESTAMP() AS dbt_processed_at

FROM bureau_staging
GROUP BY user_id
