{{ config(materialized = 'view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_home_credit', 'credit_card_balance') }}
)

SELECT
    SK_ID_PREV::INT AS prev_application_id,
    SK_ID_CURR::INT AS user_id,
    MONTHS_BALANCE::INT AS months_balance,

    -- Balance and limits
    AMT_BALANCE::FLOAT AS balance,
    AMT_CREDIT_LIMIT_ACTUAL::FLOAT AS credit_limit,
    AMT_TOTAL_RECEIVABLE::FLOAT AS total_receivable,
    AMT_RECEIVABLE_PRINCIPAL::FLOAT AS principal_receivable,
    AMT_RECIVABLE::FLOAT AS receivable,

    -- Drawing amounts
    AMT_DRAWINGS_ATM_CURRENT::FLOAT AS atm_drawings,
    AMT_DRAWINGS_CURRENT::FLOAT AS total_drawings,
    AMT_DRAWINGS_OTHER_CURRENT::FLOAT AS other_drawings,
    AMT_DRAWINGS_POS_CURRENT::FLOAT AS pos_drawings,

    -- Payment info
    AMT_INST_MIN_REGULARITY::FLOAT AS min_installment,
    AMT_PAYMENT_CURRENT::FLOAT AS current_payment,
    AMT_PAYMENT_TOTAL_CURRENT::FLOAT AS total_payment,

    -- Drawing counts
    CNT_DRAWINGS_ATM_CURRENT::INT AS atm_drawing_count,
    CNT_DRAWINGS_CURRENT::INT AS total_drawing_count,
    CNT_DRAWINGS_OTHER_CURRENT::INT AS other_drawing_count,
    CNT_DRAWINGS_POS_CURRENT::INT AS pos_drawing_count,
    CNT_INSTALMENT_MATURE_CUM::INT AS installments_matured,

    -- Contract status
    NAME_CONTRACT_STATUS::VARCHAR AS contract_status,

    -- DPD
    SK_DPD::INT AS days_past_due,
    SK_DPD_DEF::INT AS days_past_due_tolerance,

    -- Derived metrics
    CASE
        WHEN AMT_CREDIT_LIMIT_ACTUAL > 0
        THEN AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL
        ELSE NULL
    END AS utilization_ratio,

    CASE
        WHEN AMT_INST_MIN_REGULARITY > 0 AND AMT_PAYMENT_CURRENT > 0
        THEN AMT_PAYMENT_CURRENT / AMT_INST_MIN_REGULARITY
        ELSE NULL
    END AS payment_to_min_ratio,

    CASE
        WHEN AMT_CREDIT_LIMIT_ACTUAL > 0 AND AMT_DRAWINGS_CURRENT > 0
        THEN AMT_DRAWINGS_CURRENT / AMT_CREDIT_LIMIT_ACTUAL
        ELSE NULL
    END AS drawing_to_limit_ratio,

    CURRENT_TIMESTAMP() AS dbt_processed_at

FROM source_data
