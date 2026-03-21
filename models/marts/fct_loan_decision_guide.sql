WITH base AS (
    SELECT * FROM {{ ref('fct_loan_application_features') }}
),

risk AS (
    SELECT * FROM {{ ref('fct_loan_risk_assessment') }}
),

enriched AS (
    SELECT
        b.user_id,
        b.target,
        b.age_years,
        b.gender,
        b.annual_income,
        b.credit_amount,
        b.loan_annuity,
        b.goods_price,
        b.contract_type,
        b.annuity_to_income_ratio,
        b.credit_to_income_ratio,
        b.ext_source_avg,
        b.bureau_debt_ratio,
        b.max_bureau_overdue_ever,
        b.total_bureau_credits,
        b.total_bureau_overdue,
        b.prev_refused_count,
        
        r.total_risk_score,
        r.risk_level,
        r.income_burden_ratio,
        r.has_bureau_overdue_history,
        r.high_income_burden,
        r.high_cc_utilization,
        r.low_external_score,
        r.is_young_borrower,

        -- Affordability indicators
        CASE
            WHEN b.annual_income < 50000 THEN 'low'
            WHEN b.annual_income < 150000 THEN 'medium'
            ELSE 'high'
        END AS income_tier,

        CASE
            WHEN b.credit_to_income_ratio < 3 THEN 'affordable'
            WHEN b.credit_to_income_ratio < 6 THEN 'moderate'
            WHEN b.credit_to_income_ratio < 10 THEN 'stretched'
            ELSE 'overextended'
        END AS debt_burden_level,

        CASE
            WHEN b.ext_source_avg >= 0.6 THEN 'strong'
            WHEN b.ext_source_avg >= 0.4 THEN 'moderate'
            WHEN b.ext_source_avg >= 0.2 THEN 'weak'
            ELSE 'very_weak'
        END AS creditworthiness_tier,

        -- Bureau history assessment
        CASE
            WHEN b.total_bureau_credits = 0 THEN 'no_history'
            WHEN b.total_bureau_credits <= 3 THEN 'limited_history'
            WHEN b.bureau_debt_ratio < 0.5 THEN 'healthy_bureau'
            WHEN b.bureau_debt_ratio < 0.8 THEN 'moderate_bureau'
            ELSE 'concerning_bureau'
        END AS bureau_health_status,

        -- Payment history indicator
        CASE
            WHEN b.total_bureau_overdue > 0 THEN 'has_overdue'
            WHEN b.max_bureau_overdue_ever > 0 THEN 'had_overdue'
            ELSE 'clean_history'
        END AS payment_history_status

    FROM base b
    LEFT JOIN risk r ON b.user_id = r.user_id
)

SELECT
    *,
    
    -- Overall financial wellness score (0-100)
    CASE
        WHEN income_tier = 'high' AND creditworthiness_tier IN ('strong', 'moderate') THEN
            CASE risk_level
                WHEN 'low' THEN 85 + (ext_source_avg * 15)
                WHEN 'medium' THEN 60 + (ext_source_avg * 15)
                ELSE 40 + (ext_source_avg * 15)
            END
        WHEN income_tier = 'medium' AND creditworthiness_tier IN ('strong', 'moderate') THEN
            CASE risk_level
                WHEN 'low' THEN 75 + (ext_source_avg * 10)
                WHEN 'medium' THEN 50 + (ext_source_avg * 10)
                ELSE 30 + (ext_source_avg * 10)
            END
        ELSE
            CASE risk_level
                WHEN 'low' THEN 65
                WHEN 'medium' THEN 40
                ELSE 20
            END
    END AS financial_wellness_score,

    -- Loan amount recommendation (based on income and existing debt)
    CASE
        WHEN annual_income > 0 THEN
            LEAST(
                annual_income * 5,
                annual_income * (CASE WHEN debt_burden_level = 'affordable' THEN 5 WHEN debt_burden_level = 'moderate' THEN 3 WHEN debt_burden_level = 'stretched' THEN 2 ELSE 1 END)
            )
        ELSE NULL
    END AS recommended_max_loan,

    -- Monthly payment affordability
    CASE
        WHEN annual_income > 0 THEN annual_income / 12 * 0.35
        ELSE NULL
    END AS affordable_monthly_payment,

    -- Key recommendations (text-based insights)
    CASE
        WHEN total_risk_score >= 5 THEN 'High risk profile - recommend debt consolidation and credit counseling before approval'
        WHEN bureau_health_status = 'concerning_bureau' THEN 'Review bureau history - existing debt levels may impact new loan capacity'
        WHEN income_burden_ratio > 0.4 THEN 'Payment-to-income ratio is high - consider smaller loan amount or longer term'
        WHEN ext_source_avg < 0.3 THEN 'External credit scores are low - applicant may benefit from building credit first'
        WHEN debt_burden_level = 'overextended' THEN 'Current debt levels are high - approval should include debt payoff plan'
        WHEN has_bureau_overdue_history = 1 THEN 'History of late payments detected - require explanation and additional verification'
        WHEN creditworthiness_tier = 'weak' THEN 'Limited credit history - may require co-signer or larger down payment'
        WHEN prev_refused_count > 2 THEN 'Multiple previous refusals - conduct thorough review of application circumstances'
        ELSE 'Profile meets standard criteria - proceed with normal underwriting'
    END AS primary_recommendation,

    CASE
        WHEN high_income_burden = 1 THEN 'Monthly payment exceeds 40% of income - stretch loan term to reduce burden'
        WHEN high_cc_utilization = 1 THEN 'Credit card utilization is high - require payoff of existing cards as condition'
        WHEN low_external_score = 1 THEN 'External scores below average - offer credit-builder terms or higher down payment'
        WHEN bureau_debt_ratio > 0.5 THEN 'Existing bureau debt is significant - factor into debt-to-income calculation'
        WHEN is_young_borrower = 1 THEN 'Young borrower - consider extended term for lower payments while building history'
        ELSE NULL
    END AS secondary_recommendation,

    CURRENT_TIMESTAMP() AS fct_created_at

FROM enriched
