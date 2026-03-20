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

    CURRENT_TIMESTAMP() AS stg_processed_at

FROM source_data
