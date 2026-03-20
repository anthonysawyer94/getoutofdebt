WITH staging_apps AS (
    SELECT * FROM {{ ref('stg_applications')}}
)
SELECT
    *,
    COALESCE(
        (ext_source_1 + ext_source_2 + ext_source_3) / 3,
        ext_source_1, ext_source_2, ext_source_3
    ) AS ext_source_avg,

    -- Derived features
    CASE
        WHEN CREDIT_AMOUNT > 0 THEN LOAN_ANNUITY / CREDIT_AMOUNT
        ELSE NULL
    END AS annuity_to_credit_ratio,

    CASE
        WHEN ANNUAL_INCOME > 0 THEN CREDIT_AMOUNT / ANNUAL_INCOME
        ELSE NULL
    END AS credit_to_income_ratio,

    CASE
        WHEN GOODS_PRICE > 0 THEN CREDIT_AMOUNT / GOODS_PRICE
        ELSE NULL
    END AS credit_to_goods_ratio,

    CASE
        WHEN LOAN_ANNUITY > 0 AND ANNUAL_INCOME > 0
        THEN (LOAN_ANNUITY * 12) / ANNUAL_INCOME
        ELSE NULL
    END AS annuity_to_income_ratio,

    CURRENT_TIMESTAMP() AS int_app_calculated_at

FROM staging_apps