WITH source_data AS (
    SELECT * FROM {{ source('raw_home_credit', 'application_train') }}
)

SELECT
    SK_ID_CURR::INT AS user_id,
    TARGET::INT AS target,

    -- Contract info
    NAME_CONTRACT_TYPE::VARCHAR AS contract_type,

    -- Demographics
    CODE_GENDER::VARCHAR AS gender,
    FLAG_OWN_CAR::BOOLEAN AS owns_car,
    FLAG_OWN_REALTY::BOOLEAN AS owns_realty,
    CNT_CHILDREN::INT AS num_children,
    CNT_FAM_MEMBERS::INT AS num_family_members,

    -- Income and credit amounts
    AMT_INCOME_TOTAL::FLOAT AS annual_income,
    AMT_CREDIT::FLOAT AS credit_amount,
    AMT_ANNUITY::FLOAT AS loan_annuity,
    AMT_GOODS_PRICE::FLOAT AS goods_price,

    -- Income classification
    NAME_INCOME_TYPE::VARCHAR AS income_type,

    -- Education and family
    NAME_EDUCATION_TYPE::VARCHAR AS education_type,
    NAME_FAMILY_STATUS::VARCHAR AS family_status,

    -- Housing
    NAME_HOUSING_TYPE::VARCHAR AS housing_type,

    -- Age and employment- Standardizing time units and handling 'magic number' nulls
    ABS(DAYS_BIRTH::INT) / 365.25 AS age_years,
    -- Handles the 'unknown' outlier
    CASE
        WHEN DAYS_EMPLOYED = 365243 THEN NULL
        ELSE ABS(DAYS_EMPLOYED::INT) / 365.25
    END AS years_employed,

    -- Registration and ID info
    DAYS_REGISTRATION::INT AS days_since_registration,
    DAYS_ID_PUBLISH::INT AS days_since_id_publish,

    -- Contact flags
    FLAG_MOBIL::BOOLEAN AS has_mobile,
    FLAG_EMP_PHONE::BOOLEAN AS has_work_phone,
    FLAG_WORK_PHONE::BOOLEAN AS has_home_phone,
    FLAG_CONT_MOBILE::BOOLEAN AS mobile_reachable,
    FLAG_EMAIL::BOOLEAN AS has_email,

    -- Occupation
    OCCUPATION_TYPE::VARCHAR AS occupation_type,
    
    -- Region ratings
    REGION_RATING_CLIENT::INT AS region_rating,
    REGION_RATING_CLIENT_W_CITY::INT AS region_rating_with_city,

    -- Region mismatch flags
    REG_REGION_NOT_LIVE_REGION::INT AS region_live_mismatch,
    REG_REGION_NOT_WORK_REGION::INT AS region_work_mismatch,
    LIVE_REGION_NOT_WORK_REGION::INT AS live_work_mismatch,
    REG_CITY_NOT_LIVE_CITY::INT AS city_live_mismatch,
    REG_CITY_NOT_WORK_CITY::INT AS city_work_mismatch,
    LIVE_CITY_NOT_WORK_CITY::INT AS city_work_live_mismatch,

    -- Organization
    ORGANIZATION_TYPE::VARCHAR AS organization_type,

    -- External scores
    TRY_CAST(EXT_SOURCE_1 AS FLOAT) AS ext_source_1,
    TRY_CAST(EXT_SOURCE_2 AS FLOAT) AS ext_source_2,
    TRY_CAST(EXT_SOURCE_3 AS FLOAT) AS ext_source_3,

    -- Application timing
    WEEKDAY_APPR_PROCESS_START::VARCHAR AS application_weekday,
    HOUR_APPR_PROCESS_START::INT AS application_hour,

    -- Credit bureau inquiries
    COALESCE(AMT_REQ_CREDIT_BUREAU_HOUR::INT, 0) AS bureau_inquiries_hour,
    COALESCE(AMT_REQ_CREDIT_BUREAU_DAY::INT, 0) AS bureau_inquiries_day,
    COALESCE(AMT_REQ_CREDIT_BUREAU_WEEK::INT, 0) AS bureau_inquiries_week,
    COALESCE(AMT_REQ_CREDIT_BUREAU_MON::INT, 0) AS bureau_inquiries_month,
    COALESCE(AMT_REQ_CREDIT_BUREAU_QRT::INT, 0) AS bureau_inquiries_quarter,
    COALESCE(AMT_REQ_CREDIT_BUREAU_YEAR::INT, 0) AS bureau_inquiries_year,

    -- Document flags
    FLAG_DOCUMENT_3::BOOLEAN AS doc_3_provided,
    FLAG_DOCUMENT_6::BOOLEAN AS doc_6_provided,
    FLAG_DOCUMENT_8::BOOLEAN AS doc_8_provided,

    CURRENT_TIMESTAMP() AS stg_processed_at

FROM source_data
