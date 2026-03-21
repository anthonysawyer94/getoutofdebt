# Home Credit Risk

Analytics Engineering pipeline built with Snowflake and dbt that transforms raw credit data into actionable insights, powering smarter lending decisions.

## Data Lineage

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  RAW (Kaggle CSV) → L1_LANDING → L2_PROCESSING → L3_CONSUMPTION          │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Schema | Description |
|-------|--------|-------------|
| **Raw** | Kaggle CSV | Original Home Credit data (~300k applications) |
| **L1 Landing** | `L1_LANDING` | Raw tables: application_train, bureau, previous_application, credit_card_balance |
| **L2 Processing** | `L2_PROCESSING` | `stg_*` (cleaned) + `int_*` (aggregated) |
| **L3 Consumption** | `L3_CONSUMPTION` | `fct_*` (analytics-ready) |

**Why this matters:**
- **Traceability** — Every field traces back to source
- **Quality gates** — Data validated at each transformation
- **Reproducibility** — Rebuild from any layer

## Setup

### Prerequisites

- Python 3.8+ (not 3.14)
- dbt with Snowflake adapter
- Snowflake account

### 1. Install dependencies

```bash
pip install dbt-snowflake
```

### 2. Configure profiles

```bash
mkdir -p ~/.dbt
nano ~/.dbt/profiles.yml
```

Add your credentials:

```yaml
home_credit_risk:
  outputs:
    dev:
      type: snowflake
      account: [your_account_locator]
      user: [your_username]
      password: [your_password]
      role: [your_role]
      database: HOME_CREDIT_DEV
      warehouse: [your_warehouse]
      schema: L1_LANDING
      threads: 1
  target: dev
```

> **Security:** Never commit `profiles.yml` to Git. Keep it in `~/.dbt/`.

### 3. Load data

1. Download CSVs from [Kaggle](https://www.kaggle.com/competitions/home-credit-default-risk/data)
2. Upload to Snowflake via SnowSQL or UI
3. Tables: `application_train`, `bureau`, `previous_application`, `credit_card_balance`

### 4. Install packages

```bash
dbt deps
```

## Running the Project

```bash
# Verify setup
dbt debug

# Compile (no execution)
dbt compile

# Run all models
dbt run

# Run by layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Test
dbt test
dbt test --store-failures
```

## Models

### Staging (Bronze)

| Model | Source | Description |
|-------|--------|-------------|
| `stg_applications` | application_train | Main loan applications |
| `stg_bureau` | bureau | Credit bureau records |
| `stg_previous_applications` | previous_application | Prior Home Credit loans |
| `stg_credit_card_balance` | credit_card_balance | Credit card balances |

### Intermediate (Silver)

| Model | Description |
|-------|-------------|
| `int_bureau_aggregated` | Bureau credit aggregates per user |
| `int_previous_applications_aggregated` | Previous application stats |
| `int_credit_card_aggregated` | Credit card usage patterns |

### Marts (Gold)

| Model | Description |
|-------|-------------|
| `fct_loan_application_features` | 100+ features from all sources |
| `fct_loan_risk_assessment` | 12 risk indicators + risk level |
| `fct_loan_decision_guide` | Financial wellness scores + actionable recommendations for advisors |

## Key Features

- Snowflake VARIANT handling for semi-structured data
- Medallion architecture with full lineage traceability
- 100+ engineered features from 4 source tables
- 12 risk indicators for loan assessment
- External score integration (EXT_SOURCE_1/2/3)
- Decision guide with financial wellness scores and actionable recommendations

## Testing

Both generic and custom tests ensure data quality:

| Type | Examples |
|------|----------|
| Schema tests | unique, not_null, accepted_values |
| Custom assertions | referential integrity, positive amounts |

```bash
dbt test
dbt test --select assert_bureau_user_exists
```

## Documentation

```bash
dbt docs generate
dbt docs serve
```

## Project Structure

```
.
├── dbt_project.yml
├── models/
│   ├── staging/      # stg_*.sql, sources.yml, staging.yml
│   ├── intermediate/ # int_*.sql, int_models.yml
│   └── marts/       # fct_*.sql, marts.yml
├── tests/            # assert_*.sql
├── macros/          # analytics_helpers.sql, macros.yml
└── README.md
```

## Notes

- Target variable: `1` = default, `0` = repaid
- Days fields are negative (counting back from application date)
- `365243` in DAYS_EMPLOYED = unemployment indicator
