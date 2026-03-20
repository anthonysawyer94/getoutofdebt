{{ config(materialized = 'view') }}

WITH prev_app_staging AS (
    SELECT * FROM {{ ref('stg_previous_applications') }}
)

SELECT
    user_id,

    -- Application counts
    COUNT(*) AS total_previous_applications,
    COUNT(DISTINCT prev_application_id) AS unique_applications,

    -- Approval status
    SUM(CASE WHEN contract_status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN contract_status = 'Refused' THEN 1 ELSE 0 END) AS refused_count,
    SUM(CASE WHEN contract_status = 'Canceled' THEN 1 ELSE 0 END) AS canceled_count,
    SUM(CASE WHEN contract_status = 'Unused' THEN 1 ELSE 0 END) AS unused_count,

    -- Approval rate
    CASE
        WHEN COUNT(*) > 0
        THEN SUM(CASE WHEN contract_status = 'Approved' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
        ELSE NULL
    END AS approval_rate,

    -- Amounts
    AVG(requested_amount) AS avg_requested_amount,
    MAX(requested_amount) AS max_requested_amount,
    SUM(approved_credit) AS total_approved_credit,
    AVG(approved_credit) AS avg_approved_credit,

    -- Interest rates
    AVG(interest_rate_primary) AS avg_interest_rate_primary,
    AVG(interest_rate_privileged) AS avg_interest_rate_privileged,

    -- Down payment
    AVG(down_payment) AS avg_down_payment,
    AVG(down_payment_rate) AS avg_down_payment_rate,

    -- Term
    AVG(term_months) AS avg_term_months,
    MAX(term_months) AS max_term_months,
    MIN(term_months) AS min_term_months,

    -- Annuity
    AVG(annuity) AS avg_annuity,
    SUM(annuity) AS total_annuity,

    -- Client types
    SUM(CASE WHEN client_type = 'Repeater' THEN 1 ELSE 0 END) AS repeater_count,
    SUM(CASE WHEN client_type = 'New' THEN 1 ELSE 0 END) AS new_client_count,
    SUM(CASE WHEN client_type = 'Refreshed' THEN 1 ELSE 0 END) AS refreshed_count,

    -- Contract types
    COUNT(DISTINCT contract_type) AS num_contract_types,

    -- Portfolio
    COUNT(DISTINCT portfolio) AS num_portfolios,

    -- Insurance
    SUM(insurance_requested) AS insurance_requests,

    -- Days since decisions
    AVG(days_since_decision) AS avg_days_since_decision,
    MIN(days_since_decision) AS earliest_decision_days,
    MAX(days_since_decision) AS latest_decision_days,

    CURRENT_TIMESTAMP() AS int_previous_applications_aggregated_at

FROM prev_app_staging
GROUP BY user_id
