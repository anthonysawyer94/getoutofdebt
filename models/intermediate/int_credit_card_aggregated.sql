{{ config(materialized = 'view') }}

WITH cc_staging AS (
    SELECT * FROM {{ ref('stg_credit_card_balance') }}
)

SELECT
    user_id,

    -- Record counts
    COUNT(*) AS total_cc_records,
    COUNT(DISTINCT prev_application_id) AS num_credit_cards,

    -- Balance statistics
    AVG(balance) AS avg_balance,
    MAX(balance) AS max_balance,
    MIN(balance) AS min_balance,
    STDDEV(balance) AS balance_stddev,

    -- Credit limit
    AVG(credit_limit) AS avg_credit_limit,
    MAX(credit_limit) AS max_credit_limit,

    -- Utilization
    AVG(utilization_ratio) AS avg_utilization,
    MAX(utilization_ratio) AS max_utilization,

    -- Utilization categories
    SUM(CASE WHEN utilization_ratio > 0.9 THEN 1 ELSE 0 END) AS high_utilization_count,
    SUM(CASE WHEN utilization_ratio < 0.3 THEN 1 ELSE 0 END) AS low_utilization_count,

    -- Drawing activity
    SUM(atm_drawings) AS total_atm_drawings,
    SUM(total_drawings) AS total_drawings,
    AVG(total_drawings) AS avg_drawings,
    SUM(total_drawing_count) AS total_drawing_transactions,

    -- Payment behavior
    AVG(current_payment) AS avg_payment,
    MAX(current_payment) AS max_payment,
    SUM(total_payment) AS total_payments,
    AVG(total_payment) AS avg_total_payment,

    -- Payment to min ratio
    AVG(payment_to_min_ratio) AS avg_payment_to_min_ratio,

    -- DPD
    SUM(CASE WHEN days_past_due > 0 THEN 1 ELSE 0 END) AS overdue_periods,
    AVG(days_past_due) AS avg_days_past_due,
    MAX(days_past_due) AS max_days_past_due,

    -- Overdue rate
    CASE
        WHEN COUNT(*) > 0
        THEN SUM(CASE WHEN days_past_due > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
        ELSE 0
    END AS overdue_rate,

    -- Receivables
    AVG(total_receivable) AS avg_receivable,
    SUM(total_receivable) AS total_receivable,

    CURRENT_TIMESTAMP() AS dbt_processed_at

FROM cc_staging
GROUP BY user_id
