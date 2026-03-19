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
        WHEN AMT_CREDIT > 0 THEN AMT_ANNUITY / AMT_CREDIT
        ELSE NULL
    END AS annuity_to_credit_ratio,

    CASE
        WHEN AMT_INCOME_TOTAL > 0 THEN AMT_CREDIT / AMT_INCOME_TOTAL
        ELSE NULL
    END AS credit_to_income_ratio,

    CASE
        WHEN AMT_GOODS_PRICE > 0 THEN AMT_CREDIT / AMT_GOODS_PRICE
        ELSE NULL
    END AS credit_to_goods_ratio,

    CASE
        WHEN AMT_ANNUITY > 0 AND AMT_INCOME_TOTAL > 0
        THEN (AMT_ANNUITY * 12) / AMT_INCOME_TOTAL
        ELSE NULL
    END AS annuity_to_income_ratio,

    CURRENT_TIMESTAMP() AS int_app_calculated_at

FROM staging_apps