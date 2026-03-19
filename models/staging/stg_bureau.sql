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
    ABS(DAYS_CREDIT::INT) AS days_since_credit,
    DAYS_CREDIT_ENDDATE::INT AS days_until_credit_end,
    ABS(DAYS_ENDDATE_FACT::INT) AS days_since_credit_closed,
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

    CURRENT_TIMESTAMP() AS stg_processed_at

FROM source_data
