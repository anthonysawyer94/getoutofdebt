{{ config(materialized = 'table') }}

WITH application_features AS (
    SELECT * FROM {{ ref('fct_loan_application_features') }}
)

SELECT
    user_id,
    target AS loan_defaulted,

    -- Bureau risk score components
    CASE WHEN total_bureau_credits > 5 THEN 1 ELSE 0 END AS high_bureau_credit_count,
    CASE WHEN bureau_debt_ratio > 0.8 THEN 1 ELSE 0 END AS high_bureau_debt_ratio,
    CASE WHEN max_bureau_overdue_ever > 0 THEN 1 ELSE 0 END AS has_bureau_overdue_history,
    CASE WHEN bureau_prolongations > 0 THEN 1 ELSE 0 END AS has_credit_prolongations,

    -- Previous application risk
    CASE WHEN prev_refused_count > 2 THEN 1 ELSE 0 END AS multiple_refusals,
    CASE WHEN prev_approval_rate < 0.5 THEN 1 ELSE 0 END AS low_approval_rate,
    CASE WHEN prev_insurance_requests > prev_approved_count THEN 1 ELSE 0 END AS excessive_insurance,

    -- Credit card risk
    CASE WHEN avg_cc_utilization > 0.8 THEN 1 ELSE 0 END AS high_cc_utilization,
    CASE WHEN cc_overdue_rate > 0.1 THEN 1 ELSE 0 END AS cc_overdue_issues,

    -- External score risk
    CASE WHEN ext_source_avg < 0.2 THEN 1 ELSE 0 END AS very_low_external_score,
    CASE WHEN ext_source_avg < 0.4 THEN 1 ELSE 0 END AS low_external_score,

    -- Financial stress indicators
    CASE WHEN income_burden_ratio > 0.4 THEN 1 ELSE 0 END AS high_income_burden,
    CASE WHEN credit_to_income_ratio > 10 THEN 1 ELSE 0 END AS very_high_credit_to_income,

    -- Combined risk score
    (
        CASE WHEN total_bureau_credits > 5 THEN 1 ELSE 0 END +
        CASE WHEN bureau_debt_ratio > 0.8 THEN 1 ELSE 0 END +
        CASE WHEN max_bureau_overdue_ever > 0 THEN 1 ELSE 0 END +
        CASE WHEN bureau_prolongations > 0 THEN 1 ELSE 0 END +
        CASE WHEN prev_refused_count > 2 THEN 1 ELSE 0 END +
        CASE WHEN prev_approval_rate < 0.5 THEN 1 ELSE 0 END +
        CASE WHEN avg_cc_utilization > 0.8 THEN 1 ELSE 0 END +
        CASE WHEN cc_overdue_rate > 0.1 THEN 1 ELSE 0 END +
        CASE WHEN ext_source_avg < 0.2 THEN 1 ELSE 0 END +
        CASE WHEN ext_source_avg < 0.4 THEN 1 ELSE 0 END +
        CASE WHEN income_burden_ratio > 0.4 THEN 1 ELSE 0 END +
        CASE WHEN credit_to_income_ratio > 10 THEN 1 ELSE 0 END
    ) AS total_risk_score,

    -- Risk level classification
    CASE
        WHEN (
            CASE WHEN total_bureau_credits > 5 THEN 1 ELSE 0 END +
            CASE WHEN bureau_debt_ratio > 0.8 THEN 1 ELSE 0 END +
            CASE WHEN max_bureau_overdue_ever > 0 THEN 1 ELSE 0 END +
            CASE WHEN bureau_prolongations > 0 THEN 1 ELSE 0 END +
            CASE WHEN prev_refused_count > 2 THEN 1 ELSE 0 END +
            CASE WHEN prev_approval_rate < 0.5 THEN 1 ELSE 0 END +
            CASE WHEN avg_cc_utilization > 0.8 THEN 1 ELSE 0 END +
            CASE WHEN cc_overdue_rate > 0.1 THEN 1 ELSE 0 END +
            CASE WHEN ext_source_avg < 0.2 THEN 1 ELSE 0 END +
            CASE WHEN ext_source_avg < 0.4 THEN 1 ELSE 0 END +
            CASE WHEN income_burden_ratio > 0.4 THEN 1 ELSE 0 END +
            CASE WHEN credit_to_income_ratio > 10 THEN 1 ELSE 0 END
        ) >= 5 THEN 'high'
        WHEN (
            CASE WHEN total_bureau_credits > 5 THEN 1 ELSE 0 END +
            CASE WHEN bureau_debt_ratio > 0.8 THEN 1 ELSE 0 END +
            CASE WHEN max_bureau_overdue_ever > 0 THEN 1 ELSE 0 END +
            CASE WHEN bureau_prolongations > 0 THEN 1 ELSE 0 END +
            CASE WHEN prev_refused_count > 2 THEN 1 ELSE 0 END +
            CASE WHEN prev_approval_rate < 0.5 THEN 1 ELSE 0 END +
            CASE WHEN avg_cc_utilization > 0.8 THEN 1 ELSE 0 END +
            CASE WHEN cc_overdue_rate > 0.1 THEN 1 ELSE 0 END +
            CASE WHEN ext_source_avg < 0.2 THEN 1 ELSE 0 END +
            CASE WHEN ext_source_avg < 0.4 THEN 1 ELSE 0 END +
            CASE WHEN income_burden_ratio > 0.4 THEN 1 ELSE 0 END +
            CASE WHEN credit_to_income_ratio > 10 THEN 1 ELSE 0 END
        ) >= 2 THEN 'medium'
        ELSE 'low'
    END AS risk_level,

    -- Age and income flags
    CASE WHEN age_years < 25 THEN 1 ELSE 0 END AS is_young_borrower,
    CASE WHEN age_years > 60 THEN 1 ELSE 0 END AS is_senior_borrower,
    CASE WHEN annual_income < 50000 THEN 1 ELSE 0 END AS low_income,
    CASE WHEN annual_income > 300000 THEN 1 ELSE 0 END AS high_income,

    -- Credit bureau inquiry rate
    CASE
        WHEN annual_income > 0 AND credit_amount > 0
        THEN (loan_annuity * 12) / annual_income
        ELSE NULL
    END AS income_burden_ratio,

    CURRENT_TIMESTAMP() AS dbt_processed_at

FROM application_features
