WITH bureau_metrics AS (
    SELECT * FROM {{ ref('int_bureau_metrics') }}
)

SELECT
    user_id,

    -- 1. Account Volume (How much credit history do they have?)
    COUNT(*) AS total_bureau_credits,
    COUNT(DISTINCT credit_type) AS num_credit_types,
    COUNT(DISTINCT bureau_credit_id) AS unique_bureau_credits,
    SUM(CASE WHEN credit_status = 'Active' THEN 1 ELSE 0 END) AS active_credits,
    SUM(CASE WHEN credit_status = 'Closed' THEN 1 ELSE 0 END) AS closed_credits,

    -- 2. Total Debt Exposure (The "Big Numbers")
    SUM(total_credit_amount) AS total_bureau_credit_amount,
    AVG(total_credit_amount) AS avg_bureau_credit_amount,
    MAX(total_credit_amount) AS max_bureau_credit_amount,
    MIN(total_credit_amount) AS min_bureau_credit_amount,

    -- Debt
    SUM(current_debt) AS total_bureau_debt,
    AVG(current_debt) AS avg_bureau_debt,

    -- Annuity
    AVG(annuity) AS avg_annuity,
    SUM(annuity) AS total_annuity,

    -- Overdue
    SUM(overdue_amount) AS total_overdue_amount,
    MAX(max_overdue_amount) AS max_overdue_ever,
    AVG(days_overdue) AS avg_days_overdue,
    SUM(num_times_prolonged) AS total_prolongations,

    -- Credit Age
    AVG(days_since_credit) AS avg_days_since_credit,
    MIN(days_since_credit) AS newest_credit_account_days,
    MAX(days_since_credit) AS oldest_credit_account_days,

    -- Debt ratios
    CASE
        WHEN SUM(total_credit_amount) > 0
        THEN SUM(current_debt) / SUM(total_credit_amount)
        ELSE NULL
    END AS overall_debt_ratio,

    CURRENT_TIMESTAMP() AS int_bureau_aggregated_at

FROM bureau_metrics
GROUP BY user_id
