# Home Credit Default Risk - dbt Project

A dbt project for analyzing the Home Credit Default Risk dataset from Kaggle. This project transforms raw loan application data into analytics-ready features for risk assessment.

## Data Source

Data is sourced from the [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data) Kaggle competition. The dataset contains ~300k loan applications with rich historical data.

## Architecture

### Medallion Structure

| Layer      | Models  | Description                |
| ---------- | ------- | -------------------------- |
| **Bronze** | `stg_*` | Raw data typed and cleaned |
| **Silver** | `int_*` | Aggregated features        |
| **Gold**   | `fct_*` | Analytics-ready tables     |

### Data Model

```
┌─────────────────────┐
│  application_train  │ (Main table with TARGET)
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌─────────────────────┐
│ bureau  │ │ previous_application│
└─────────┘ └─────────┬───────────┘
                      ▼
            ┌─────────────────────┐
            │ credit_card_balance │
            └─────────────────────┘
```

## Getting Started

### Prerequisites

- Python 3.8.x <= & => 3.14.x (dbt doesn't work with 3.14 yet)
- dbt (with Snowflake adapter)
- Snowflake account (can sign up for a free account)

### Setup

1. **Install dependencies**

   ```bash
   pip install dbt-snowflake
   ```

2. **Database Configuration**

   ```bash
   #Create directory (if doesn't exist)
   mkdir -p ~/.dbt

   #Create/Edit the file
   nano ~/.dbt/profiles.yml
   ```

   - Copy and paste following template: (replace bracketed values with your Snowflake credentials)

   ⚠️ Security Warning: Never commit your profiles.yml or any file containing passwords to a public GitHub repository. This file should remain in your local ~/.dbt/ folder.

   ```yaml
   home_credit_risk:
     outputs:
       dev:
         type: snowflake
         account: [your_account_locator]
         user: [your_username]
         password: [your_password]
         role: [ACCOUNTADMIN or your specific role]
         database: HOME_CREDIT_DEV
         warehouse: [your_warehouse_name]
         schema: L1_LANDING
         threads: 1
     target: dev
   ```

3. **Install dbt packages**

   ```bash
   dbt deps
   ```

4. **Load data to Snowflake**
   - Download CSVs from Kaggle
   - Upload to Snowflake via SnowSQL or Snowflake UI
   - Create tables matching the schema

## Running the Project

```bash
# Verify configuration
dbt debug

# Compile all models (no execution)
dbt compile

# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run tests
dbt test
dbt test --store-failures
```

## Models

### Staging (Bronze)

| Model                       | Source Table           | Description             |
| --------------------------- | ---------------------- | ----------------------- |
| `stg_applications`          | `application_train`    | Main loan applications  |
| `stg_bureau`                | `bureau`               | Credit bureau records   |
| `stg_previous_applications` | `previous_application` | Prior Home Credit loans |
| `stg_credit_card_balance`   | `credit_card_balance`  | Credit card balances    |

### Intermediate (Silver)

| Model                                  | Description                       |
| -------------------------------------- | --------------------------------- |
| `int_bureau_aggregated`                | Bureau credit aggregates per user |
| `int_previous_applications_aggregated` | Previous application stats        |
| `int_credit_card_aggregated`           | Credit card usage patterns        |

### Marts (Gold)

| Model                           | Description                     |
| ------------------------------- | ------------------------------- |
| `fct_loan_application_features` | Complete feature set            |
| `fct_loan_risk_assessment`      | Risk scoring and classification |

## Key Features

- **100+ engineered features** from 4 source tables
- **Credit bureau history** analysis (previous credits from other institutions)
- **Previous application** analysis (prior Home Credit loan behavior)
- **Credit card** utilization and payment patterns
- **Risk scoring** with 12 risk indicators
- **External score integration** (EXT_SOURCE_1/2/3)

## Testing

```bash
# Run all tests
dbt test

# Run specific test
dbt test --select assert_positive_amounts

# Store failures for debugging
dbt test --store-failures
```

## Documentation

```bash
# Generate docs
dbt docs generate

# Serve locally
dbt docs serve
```

## Project Structure

```
.
├── dbt_project.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   └── stg_*.sql
│   ├── intermediate/
│   │   └── int_*_aggregated.sql
│   └── marts/
│       └── fct_*.sql
├── tests/
│   └── assert_*.sql
├── macros/
│   └── analytics_helpers.sql
|   └── generate_schema_name.sql
└── README.md
```

## Notes

- Raw CSV data should be loaded to Snowflake tables matching source names
- Target variable (loan_defaulted): `1` = default, `0` = repaid
- Days fields are negative (counting backwards from application date)
- 365243 in DAYS_EMPLOYED indicates unemployment
