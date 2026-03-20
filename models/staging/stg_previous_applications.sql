WITH source_data AS (
    SELECT * FROM {{ source('raw_home_credit', 'previous_application') }}
)

SELECT
    SK_ID_PREV::INT AS prev_application_id,
    SK_ID_CURR::INT AS user_id,

    -- Contract type
    NAME_CONTRACT_TYPE::VARCHAR AS contract_type,
    NAME_PRODUCT_TYPE::VARCHAR AS product_type,

    -- Amounts
    AMT_ANNUITY::FLOAT AS annuity,
    AMT_APPLICATION::FLOAT AS requested_amount,
    AMT_CREDIT::FLOAT AS approved_credit,
    AMT_DOWN_PAYMENT::FLOAT AS down_payment,
    AMT_GOODS_PRICE::FLOAT AS goods_price,

    -- Approval status
    NAME_CONTRACT_STATUS::VARCHAR AS contract_status,
    FLAG_LAST_APPL_PER_CONTRACT::VARCHAR AS is_last_application,
    NFLAG_LAST_APPL_IN_DAY::INT AS is_last_in_day,

    -- Rates
    RATE_DOWN_PAYMENT::FLOAT AS down_payment_rate,
    TRY_CAST(RATE_INTEREST_PRIMARY AS FLOAT) AS interest_rate_primary,
    TRY_CAST(RATE_INTEREST_PRIVILEGED AS FLOAT) AS interest_rate_privileged,

    -- Purpose
    NAME_CASH_LOAN_PURPOSE::VARCHAR AS loan_purpose,
    NAME_GOODS_CATEGORY::VARCHAR AS goods_category,

    -- Client info
    NAME_CLIENT_TYPE::VARCHAR AS client_type,
    NAME_TYPE_SUITE::VARCHAR AS accompanying_type,

    -- Portfolio and seller
    NAME_PORTFOLIO::VARCHAR AS portfolio,
    NAME_SELLER_INDUSTRY::VARCHAR AS seller_industry,
    SELLERPLACE_AREA::INT AS seller_area,
    PRODUCT_COMBINATION::VARCHAR AS product_combination,

    -- Timing
    WEEKDAY_APPR_PROCESS_START::VARCHAR AS application_weekday,
    HOUR_APPR_PROCESS_START::INT AS application_hour,
    DAYS_DECISION::INT AS days_since_decision,
    CNT_PAYMENT::INT AS term_months,

    -- Loan lifecycle dates
    DAYS_FIRST_DRAWING::FLOAT AS days_first_drawing,
    DAYS_FIRST_DUE::FLOAT AS days_first_due,
    DAYS_LAST_DUE_1ST_VERSION::FLOAT AS days_last_due_first_version,
    DAYS_LAST_DUE::FLOAT AS days_last_due,
    DAYS_TERMINATION::FLOAT AS days_termination,

    -- Insurance
    NFLAG_INSURED_ON_APPROVAL::INT AS insurance_requested,

    -- Derived metrics
    CASE
        WHEN AMT_CREDIT > 0 THEN AMT_ANNUITY / AMT_CREDIT
        ELSE NULL
    END AS annuity_to_credit_ratio,

    CASE
        WHEN AMT_APPLICATION > 0 THEN AMT_CREDIT / AMT_APPLICATION
        ELSE NULL
    END AS approval_rate,

    CASE
        WHEN AMT_GOODS_PRICE > 0 THEN AMT_DOWN_PAYMENT / AMT_GOODS_PRICE
        ELSE NULL
    END AS down_payment_rate_goods,

    CURRENT_TIMESTAMP() AS stg_processed_at

FROM source_data
