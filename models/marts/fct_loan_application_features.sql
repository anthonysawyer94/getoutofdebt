{{ config(materialized = 'table') }}

WITH application AS (
    SELECT * FROM {{ ref('stg_applications') }}
),

bureau_agg AS (
    SELECT * FROM {{ ref('int_bureau_aggregated') }}
),

prev_app_agg AS (
    SELECT * FROM {{ ref('int_previous_applications_aggregated') }}
),

cc_agg AS (
    SELECT * FROM {{ ref('int_credit_card_aggregated') }}
),

combined AS (
    SELECT
        app.user_id,
        app.target,

        -- Application demographics
        app.age_years,
        app.gender,
        app.owns_car,
        app.owns_realty,
        app.num_children,
        app.num_family_members,
        app.income_type,
        app.education_type,
        app.family_status,
        app.housing_type,
        app.occupation_type,
        app.organization_type,

        -- Application financials
        app.annual_income,
        app.credit_amount,
        app.loan_annuity,
        app.goods_price,
        app.contract_type,
        app.annuity_to_credit_ratio,
        app.credit_to_income_ratio,
        app.annuity_to_income_ratio,

        -- Region info
        app.region_rating,
        app.region_live_mismatch,
        app.region_work_mismatch,
        app.city_live_mismatch,
        app.city_work_mismatch,

        -- Employment
        app.years_employed,

        -- External scores
        app.ext_source_1,
        app.ext_source_2,
        app.ext_source_3,
        app.ext_source_avg,

        -- Bureau info
        COALESCE(bureau.total_bureau_credits, 0) AS total_bureau_credits,
        COALESCE(bureau.active_credits, 0) AS active_bureau_credits,
        COALESCE(bureau.closed_credits, 0) AS closed_bureau_credits,
        COALESCE(bureau.total_bureau_credit_amount, 0) AS total_bureau_credit_amount,
        COALESCE(bureau.total_bureau_debt, 0) AS total_bureau_debt,
        COALESCE(bureau.overall_debt_ratio, 0) AS bureau_debt_ratio,
        COALESCE(bureau.total_overdue_amount, 0) AS total_bureau_overdue,
        COALESCE(bureau.max_overdue_ever, 0) AS max_bureau_overdue_ever,
        COALESCE(bureau.total_prolongations, 0) AS bureau_prolongations,

        -- Previous applications
        COALESCE(prev.total_previous_applications, 0) AS total_prev_applications,
        COALESCE(prev.approved_count, 0) AS prev_approved_count,
        COALESCE(prev.refused_count, 0) AS prev_refused_count,
        COALESCE(prev.approval_rate, 0) AS prev_approval_rate,
        COALESCE(prev.total_approved_credit, 0) AS total_prev_approved_credit,
        COALESCE(prev.avg_interest_rate_primary, 0) AS avg_prev_interest_rate,
        COALESCE(prev.avg_down_payment_rate, 0) AS avg_prev_down_payment_rate,
        COALESCE(prev.avg_term_months, 0) AS avg_prev_term_months,
        COALESCE(prev.insurance_requests, 0) AS prev_insurance_requests,

        -- Credit card
        COALESCE(cc.total_cc_records, 0) AS total_cc_records,
        COALESCE(cc.avg_balance, 0) AS avg_cc_balance,
        COALESCE(cc.avg_credit_limit, 0) AS avg_cc_limit,
        COALESCE(cc.avg_utilization, 0) AS avg_cc_utilization,
        COALESCE(cc.total_drawings, 0) AS total_cc_drawings,
        COALESCE(cc.avg_payment, 0) AS avg_cc_payment,
        COALESCE(cc.overdue_rate, 0) AS cc_overdue_rate,
        COALESCE(cc.max_days_past_due, 0) AS cc_max_dpd,

        CURRENT_TIMESTAMP() AS fct_created_at

    FROM application app
    LEFT JOIN bureau_agg bureau ON app.user_id = bureau.user_id
    LEFT JOIN prev_app_agg prev ON app.user_id = prev.user_id
    LEFT JOIN cc_agg cc ON app.user_id = cc.user_id
)

SELECT
    *,

    -- Derived risk indicators
    CASE
        WHEN annual_income > 0 AND credit_amount > 0
        THEN (loan_annuity * 12) / annual_income
        ELSE NULL
    END AS income_burden_ratio,

    CASE
        WHEN total_bureau_credits > 0
        THEN active_bureau_credits * 1.0 / total_bureau_credits
        ELSE 0
    END AS active_credit_ratio,

    CASE
        WHEN cc_overdue_rate IS NOT NULL
        THEN cc_overdue_rate
        ELSE 0
    END AS credit_card_risk_indicator,

    -- Combined external score indicator
    CASE
        WHEN ext_source_avg < 0.2 THEN 'very_low'
        WHEN ext_source_avg < 0.4 THEN 'low'
        WHEN ext_source_avg < 0.6 THEN 'medium'
        WHEN ext_source_avg < 0.8 THEN 'high'
        ELSE 'very_high'
    END AS external_score_band

FROM combined
