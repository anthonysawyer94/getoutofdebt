{{ config(materialized = 'view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_home_credit', 'bureau') }}
)

SELECT
    SK_ID_CURR::INT AS user_id,
    SK_ID_BUREAU::INT AS bureau_credit_id,

    -- Credit status
    CREDIT_ACTIVE::VARCHAR AS credit_status,
    CREDIT_CURRENCY::VARCHAR AS credit_currency,
    CREDIT_TYPE::VARCHAR AS credit_type,

    -- Duration and timing
    DAYS_CREDIT::INT AS days_since_credit,
    DAYS_CREDIT_ENDDATE::INT AS days_until_credit_end,
    DAYS_ENDDATE_FACT::INT AS days_since_credit_closed,
    DAYS_CREDIT_UPDATE::INT AS days_since_credit_update,

    -- Overdue metrics
    CREDIT_DAY_OVERDUE::INT AS days_overdue,
    AMT_CREDIT_MAX_OVERDUE::FLOAT AS max_overdue_amount,
    CNT_CREDIT_PROLONG::INT AS num_times_prolonged,

    -- Amounts
    AMT_CREDIT_SUM::FLOAT AS total_credit_amount,
    AMT_CREDIT_SUM_DEBT::FLOAT AS current_debt,
    AMT_CREDIT_SUM_LIMIT::FLOAT AS credit_limit,
    AMT_CREDIT_SUM_OVERDUE::FLOAT AS overdue_amount,
    AMT_ANNUITY::FLOAT AS annuity,

    -- Derived metrics
    CASE
        WHEN AMT_CREDIT_SUM > 0 THEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM
        ELSE NULL
    END AS debt_to_credit_ratio,

    CASE
        WHEN AMT_CREDIT_SUM > 0 THEN AMT_CREDIT_SUM_OVERDUE / AMT_CREDIT_SUM
        ELSE NULL
    END AS overdue_to_credit_ratio,

    CASE
        WHEN AMT_ANNUITY > 0 AND AMT_CREDIT_SUM > 0
        THEN AMT_CANNUITY / AMT_CREDIT_SUM
        ELSE NULL
    END AS annuity_to_credit_ratio,

    CURRENT_TIMESTAMP() AS dbt_processed_at

FROM source_data
