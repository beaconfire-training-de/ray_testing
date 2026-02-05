"""
Stock Dimensional Model ETL - QA Testing V3 (SCD Type 2)
========================================================
This DAG performs ETL operations to transform stock data from source tables
into a star schema dimensional model.

V3 Changes: 
- Merged Symbols table into dim_company (from V2)
- Added SCD Type 2 for tracking historical changes in:
  * ceo
  * company_name
  * industry
  * sector
  * exchange

Source Tables:
- US_STOCK_DAILY.DCCM.Company_Profile
- US_STOCK_DAILY.DCCM.Stock_History
- US_STOCK_DAILY.DCCM.Symbols

Target Tables:
- AIRFLOW0105.DEV.dim_company_QA_v3 (with SCD Type 2)
- AIRFLOW0105.DEV.dim_date_QA_v3
- AIRFLOW0105.DEV.fact_stock_daily_QA_v3
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    'owner': 'QA_testing_v3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_dimensional_model_QA_v3',
    default_args=default_args,
    description='ETL pipeline with SCD Type 2 - QA Testing V3',
    schedule_interval='@daily',
    catchup=False,
    tags=['QA', 'stock', 'dimensional_model', 'testing', 'v3', 'scd2'],
)

# =============================================================================
# SQL Statements
# =============================================================================

# -----------------------------------------------------------------------------
# Task 1: Create Tables (with SCD Type 2 structure for dim_company)
# -----------------------------------------------------------------------------
SQL_CREATE_DIM_COMPANY = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.dim_company_QA_v3 (
    company_key INT AUTOINCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    company_name VARCHAR(512),
    industry VARCHAR(64),
    sector VARCHAR(64),
    exchange VARCHAR(64),
    ceo VARCHAR(64),
    website VARCHAR(64),
    description VARCHAR(2048),
    mktcap NUMBER(38,0),
    beta NUMBER(18,8),
    last_div NUMBER(18,8),
    dcf NUMBER(18,8),
    
    -- SCD Type 2 fields
    effective_date DATE DEFAULT CURRENT_DATE(),
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

SQL_CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.dim_date_QA_v3 (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week VARCHAR(10),
    day_of_week_num INT,
    is_weekend BOOLEAN,
    CONSTRAINT uk_dim_date_fulldate_QA_v3 UNIQUE (full_date)
);
"""

SQL_CREATE_FACT_STOCK = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.fact_stock_daily_QA_v3 (
    stock_key INT AUTOINCREMENT PRIMARY KEY,
    company_key INT NOT NULL,
    date_key INT NOT NULL,
    open_price NUMBER(18,8),
    high_price NUMBER(18,8),
    low_price NUMBER(18,8),
    close_price NUMBER(18,8),
    adj_close NUMBER(18,8),
    volume NUMBER(38,8),
    price_change NUMBER(18,8),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_fact_company_QA_v3 FOREIGN KEY (company_key) REFERENCES AIRFLOW0105.DEV.dim_company_QA_v3(company_key),
    CONSTRAINT fk_fact_date_QA_v3 FOREIGN KEY (date_key) REFERENCES AIRFLOW0105.DEV.dim_date_QA_v3(date_key),
    CONSTRAINT uk_fact_stock_daily_QA_v3 UNIQUE (company_key, date_key)
);
"""

# -----------------------------------------------------------------------------
# Task 2: Load/Update Dimension Tables
# -----------------------------------------------------------------------------

# SCD Type 2 Step 1: Expire old records that have changes in tracked fields
SQL_SCD2_EXPIRE_OLD_RECORDS = """
UPDATE AIRFLOW0105.DEV.dim_company_QA_v3 AS target
SET 
    expiration_date = CURRENT_DATE(),
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP()
WHERE target.is_current = TRUE
AND target.symbol IN (
    SELECT s.SYMBOL
    FROM US_STOCK_DAILY.DCCM.Symbols s
    LEFT JOIN US_STOCK_DAILY.DCCM.Company_Profile cp 
        ON s.SYMBOL = cp.SYMBOL
    WHERE s.SYMBOL IS NOT NULL
)
AND EXISTS (
    SELECT 1
    FROM (
        SELECT 
            s.SYMBOL,
            COALESCE(cp.COMPANYNAME, s.NAME) AS COMPANYNAME,
            cp.INDUSTRY,
            cp.SECTOR,
            COALESCE(cp.EXCHANGE, s.EXCHANGE) AS EXCHANGE,
            cp.CEO
        FROM US_STOCK_DAILY.DCCM.Symbols s
        LEFT JOIN US_STOCK_DAILY.DCCM.Company_Profile cp 
            ON s.SYMBOL = cp.SYMBOL
        WHERE s.SYMBOL IS NOT NULL
    ) AS source
    WHERE source.SYMBOL = target.symbol
    AND (
        -- SCD Type 2: Track changes in these fields
        COALESCE(target.company_name, '') != COALESCE(source.COMPANYNAME, '') OR
        COALESCE(target.industry, '') != COALESCE(source.INDUSTRY, '') OR
        COALESCE(target.sector, '') != COALESCE(source.SECTOR, '') OR
        COALESCE(target.exchange, '') != COALESCE(source.EXCHANGE, '') OR
        COALESCE(target.ceo, '') != COALESCE(source.CEO, '')
    )
);
"""

# SCD Type 2 Step 2: Insert new records (new symbols OR new versions of changed records)
SQL_SCD2_INSERT_NEW_RECORDS = """
INSERT INTO AIRFLOW0105.DEV.dim_company_QA_v3 (
    symbol,
    company_name,
    industry,
    sector,
    exchange,
    ceo,
    website,
    description,
    mktcap,
    beta,
    last_div,
    dcf,
    effective_date,
    expiration_date,
    is_current
)
SELECT 
    source.SYMBOL,
    source.COMPANYNAME,
    source.INDUSTRY,
    source.SECTOR,
    source.EXCHANGE,
    source.CEO,
    source.WEBSITE,
    source.DESCRIPTION,
    source.MKTCAP,
    source.BETA,
    source.LASTDIV,
    source.DCF,
    CURRENT_DATE(),
    '9999-12-31',
    TRUE
FROM (
    SELECT DISTINCT
        s.SYMBOL,
        COALESCE(cp.COMPANYNAME, s.NAME) AS COMPANYNAME,
        cp.INDUSTRY,
        cp.SECTOR,
        COALESCE(cp.EXCHANGE, s.EXCHANGE) AS EXCHANGE,
        cp.CEO,
        cp.WEBSITE,
        cp.DESCRIPTION,
        cp.MKTCAP,
        cp.BETA,
        cp.LASTDIV,
        cp.DCF
    FROM US_STOCK_DAILY.DCCM.Symbols s
    LEFT JOIN US_STOCK_DAILY.DCCM.Company_Profile cp 
        ON s.SYMBOL = cp.SYMBOL
    WHERE s.SYMBOL IS NOT NULL
) AS source
WHERE NOT EXISTS (
    -- Only insert if there's no current record for this symbol
    SELECT 1 
    FROM AIRFLOW0105.DEV.dim_company_QA_v3 AS target
    WHERE target.symbol = source.SYMBOL
    AND target.is_current = TRUE
);
"""

# SCD Type 1: Update non-tracked fields (mktcap, beta, etc.) on current records
SQL_SCD1_UPDATE_NONTRACKED = """
UPDATE AIRFLOW0105.DEV.dim_company_QA_v3 AS target
SET 
    website = source.WEBSITE,
    description = source.DESCRIPTION,
    mktcap = source.MKTCAP,
    beta = source.BETA,
    last_div = source.LASTDIV,
    dcf = source.DCF,
    updated_at = CURRENT_TIMESTAMP()
FROM (
    SELECT DISTINCT
        s.SYMBOL,
        cp.WEBSITE,
        cp.DESCRIPTION,
        cp.MKTCAP,
        cp.BETA,
        cp.LASTDIV,
        cp.DCF
    FROM US_STOCK_DAILY.DCCM.Symbols s
    LEFT JOIN US_STOCK_DAILY.DCCM.Company_Profile cp 
        ON s.SYMBOL = cp.SYMBOL
    WHERE s.SYMBOL IS NOT NULL
) AS source
WHERE target.symbol = source.SYMBOL
AND target.is_current = TRUE
AND (
    COALESCE(target.mktcap, 0) != COALESCE(source.MKTCAP, 0) OR
    COALESCE(target.beta, 0) != COALESCE(source.BETA, 0) OR
    COALESCE(target.last_div, 0) != COALESCE(source.LASTDIV, 0) OR
    COALESCE(target.dcf, 0) != COALESCE(source.DCF, 0)
);
"""

SQL_MERGE_DIM_DATE = """
MERGE INTO AIRFLOW0105.DEV.dim_date_QA_v3 AS target
USING (
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(DATE, 'YYYYMMDD')) AS date_key,
        DATE AS full_date,
        YEAR(DATE) AS year,
        MONTH(DATE) AS month,
        DAY(DATE) AS day,
        QUARTER(DATE) AS quarter,
        DAYNAME(DATE) AS day_of_week,
        DAYOFWEEK(DATE) AS day_of_week_num,
        CASE WHEN DAYOFWEEK(DATE) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM US_STOCK_DAILY.DCCM.Stock_History
    WHERE DATE IS NOT NULL
) AS source
ON target.date_key = source.date_key

WHEN NOT MATCHED THEN INSERT (
    date_key,
    full_date,
    year,
    month,
    day,
    quarter,
    day_of_week,
    day_of_week_num,
    is_weekend
) VALUES (
    source.date_key,
    source.full_date,
    source.year,
    source.month,
    source.day,
    source.quarter,
    source.day_of_week,
    source.day_of_week_num,
    source.is_weekend
);
"""

# -----------------------------------------------------------------------------
# Task 3: Load Fact Table (JOIN with current dimension records only)
# -----------------------------------------------------------------------------
SQL_MERGE_FACT_STOCK = """
MERGE INTO AIRFLOW0105.DEV.fact_stock_daily_QA_v3 AS target
USING (
    SELECT
        dc.company_key,
        dd.date_key,
        sh.OPEN AS open_price,
        sh.HIGH AS high_price,
        sh.LOW AS low_price,
        sh.CLOSE AS close_price,
        sh.ADJCLOSE AS adj_close,
        sh.VOLUME AS volume,
        (sh.CLOSE - sh.OPEN) AS price_change
    FROM US_STOCK_DAILY.DCCM.Stock_History sh
    INNER JOIN AIRFLOW0105.DEV.dim_company_QA_v3 dc 
        ON sh.SYMBOL = dc.symbol
        AND dc.is_current = TRUE  -- Only join with current dimension record!
    INNER JOIN AIRFLOW0105.DEV.dim_date_QA_v3 dd 
        ON sh.DATE = dd.full_date
    WHERE sh.SYMBOL IS NOT NULL AND sh.DATE IS NOT NULL
) AS source
ON target.company_key = source.company_key 
   AND target.date_key = source.date_key

WHEN MATCHED AND (
    target.open_price != source.open_price OR
    target.high_price != source.high_price OR
    target.low_price != source.low_price OR
    target.close_price != source.close_price OR
    target.adj_close != source.adj_close OR
    target.volume != source.volume
) THEN UPDATE SET
    target.open_price = source.open_price,
    target.high_price = source.high_price,
    target.low_price = source.low_price,
    target.close_price = source.close_price,
    target.adj_close = source.adj_close,
    target.volume = source.volume,
    target.price_change = source.price_change

WHEN NOT MATCHED THEN INSERT (
    company_key,
    date_key,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close,
    volume,
    price_change
) VALUES (
    source.company_key,
    source.date_key,
    source.open_price,
    source.high_price,
    source.low_price,
    source.close_price,
    source.adj_close,
    source.volume,
    source.price_change
);
"""

# -----------------------------------------------------------------------------
# Task 4: Data Validation Queries
# -----------------------------------------------------------------------------
SQL_VALIDATE_ROW_COUNTS = """
-- Validation: Check row counts
SELECT 'dim_company_QA_v3 (total)' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_company_QA_v3
UNION ALL
SELECT 'dim_company_QA_v3 (current)' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_company_QA_v3 WHERE is_current = TRUE
UNION ALL
SELECT 'dim_company_QA_v3 (historical)' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_company_QA_v3 WHERE is_current = FALSE
UNION ALL
SELECT 'dim_date_QA_v3' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_date_QA_v3
UNION ALL
SELECT 'fact_stock_daily_QA_v3' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v3
UNION ALL
SELECT 'source_symbols' AS table_name, COUNT(*) AS row_count FROM US_STOCK_DAILY.DCCM.Symbols
UNION ALL
SELECT 'source_stock_history' AS table_name, COUNT(*) AS row_count FROM US_STOCK_DAILY.DCCM.Stock_History;
"""

SQL_VALIDATE_REFERENTIAL_INTEGRITY = """
-- Validation: Check for orphan records in fact table
SELECT 
    'orphan_company_keys' AS check_name,
    COUNT(*) AS orphan_count
FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v3 f
LEFT JOIN AIRFLOW0105.DEV.dim_company_QA_v3 c ON f.company_key = c.company_key
WHERE c.company_key IS NULL

UNION ALL

SELECT 
    'orphan_date_keys' AS check_name,
    COUNT(*) AS orphan_count
FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v3 f
LEFT JOIN AIRFLOW0105.DEV.dim_date_QA_v3 d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
"""

SQL_VALIDATE_SCD2 = """
-- Validation: Check SCD Type 2 integrity
-- Each symbol should have exactly one current record
SELECT 
    'symbols_with_multiple_current' AS check_name,
    COUNT(*) AS issue_count
FROM (
    SELECT symbol, COUNT(*) as cnt
    FROM AIRFLOW0105.DEV.dim_company_QA_v3
    WHERE is_current = TRUE
    GROUP BY symbol
    HAVING COUNT(*) > 1
)
UNION ALL
SELECT 
    'symbols_with_no_current' AS check_name,
    COUNT(*) AS issue_count
FROM (
    SELECT DISTINCT symbol 
    FROM AIRFLOW0105.DEV.dim_company_QA_v3
    WHERE symbol NOT IN (
        SELECT symbol FROM AIRFLOW0105.DEV.dim_company_QA_v3 WHERE is_current = TRUE
    )
);
"""

# =============================================================================
# Airflow Tasks
# =============================================================================

# Task 1: Create Tables
create_dim_company = SnowflakeOperator(
    task_id='create_dim_company',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_CREATE_DIM_COMPANY,
    dag=dag,
)

create_dim_date = SnowflakeOperator(
    task_id='create_dim_date',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_CREATE_DIM_DATE,
    dag=dag,
)

create_fact_stock = SnowflakeOperator(
    task_id='create_fact_stock',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_CREATE_FACT_STOCK,
    dag=dag,
)

# Task 2: Load Dimension Tables
# SCD Type 2 for dim_company (3 steps)
scd2_expire_old = SnowflakeOperator(
    task_id='scd2_expire_old_records',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_SCD2_EXPIRE_OLD_RECORDS,
    dag=dag,
)

scd2_insert_new = SnowflakeOperator(
    task_id='scd2_insert_new_records',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_SCD2_INSERT_NEW_RECORDS,
    dag=dag,
)

scd1_update_nontracked = SnowflakeOperator(
    task_id='scd1_update_nontracked_fields',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_SCD1_UPDATE_NONTRACKED,
    dag=dag,
)

load_dim_date = SnowflakeOperator(
    task_id='load_dim_date',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_MERGE_DIM_DATE,
    dag=dag,
)

# Task 3: Load Fact Table
load_fact_stock = SnowflakeOperator(
    task_id='load_fact_stock',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_MERGE_FACT_STOCK,
    dag=dag,
)

# Task 4: Validate Data
validate_row_counts = SnowflakeOperator(
    task_id='validate_row_counts',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_VALIDATE_ROW_COUNTS,
    dag=dag,
)

validate_referential_integrity = SnowflakeOperator(
    task_id='validate_referential_integrity',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_VALIDATE_REFERENTIAL_INTEGRITY,
    dag=dag,
)

validate_scd2 = SnowflakeOperator(
    task_id='validate_scd2_integrity',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_VALIDATE_SCD2,
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================
# 
# DAG Flow:
#
#   create_dim_company ──┬──► create_fact_stock
#                        │
#   create_dim_date ─────┘
#          │
#          ▼
#   create_dim_company ──► scd2_expire_old ──► scd2_insert_new ──► scd1_update_nontracked ──┐
#                                                                                           │
#   create_dim_date ──► load_dim_date ──────────────────────────────────────────────────────┤
#                                                                                           │
#   create_fact_stock ──────────────────────────────────────────────────────────────────────┤
#                                                                                           ▼
#                                                                                    load_fact_stock
#                                                                                           │
#                                                                                           ▼
#                                                                              validate_row_counts
#                                                                                           │
#                                                                                           ▼
#                                                                         validate_referential_integrity
#                                                                                           │
#                                                                                           ▼
#                                                                              validate_scd2_integrity

# Step 1: Create dimension tables first, then fact table (FK constraint)
[create_dim_company, create_dim_date] >> create_fact_stock

# Step 2a: SCD Type 2 process for dim_company (must be sequential)
create_dim_company >> scd2_expire_old >> scd2_insert_new >> scd1_update_nontracked

# Step 2b: Load dim_date (can run in parallel with SCD process)
create_dim_date >> load_dim_date

# Step 3: Load fact table after all dimensions are ready
[scd1_update_nontracked, load_dim_date, create_fact_stock] >> load_fact_stock

# Step 4: Validate after fact table is loaded
load_fact_stock >> validate_row_counts >> validate_referential_integrity >> validate_scd2
