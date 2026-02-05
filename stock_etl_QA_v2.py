"""
Stock Dimensional Model ETL - QA Testing V2
============================================
This DAG performs ETL operations to transform stock data from source tables
into a star schema dimensional model.

V2 Changes: Merged Symbols table into dim_company (Symbols as main table to keep all symbols data)

Source Tables:
- US_STOCK_DAILY.DCCM.Company_Profile
- US_STOCK_DAILY.DCCM.Stock_History
- US_STOCK_DAILY.DCCM.Symbols

Target Tables:
- AIRFLOW0105.DEV.dim_company_QA_v2
- AIRFLOW0105.DEV.dim_date_QA_v2
- AIRFLOW0105.DEV.fact_stock_daily_QA_v2
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    'owner': 'QA_testing_v2',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_dimensional_model_QA_v2',
    default_args=default_args,
    description='ETL pipeline for stock data dimensional model - QA Testing V2 (Symbols merged)',
    schedule_interval='@daily',
    catchup=False,
    tags=['QA', 'stock', 'dimensional_model', 'testing', 'v2'],
)

# =============================================================================
# SQL Statements
# =============================================================================

# -----------------------------------------------------------------------------
# Task 1: Create Dimension Tables and Fact Table (if not exists)
# -----------------------------------------------------------------------------
SQL_CREATE_DIM_COMPANY = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.dim_company_QA_v2 (
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
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uk_dim_company_symbol_QA UNIQUE (symbol)
);
"""

SQL_CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.dim_date_QA_v2 (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week VARCHAR(10),
    day_of_week_num INT,
    is_weekend BOOLEAN,
    CONSTRAINT uk_dim_date_fulldate_QA UNIQUE (full_date)
);
"""

SQL_CREATE_FACT_STOCK = """
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.fact_stock_daily_QA_v2 (
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
    CONSTRAINT fk_fact_company_QA FOREIGN KEY (company_key) REFERENCES AIRFLOW0105.DEV.dim_company_QA_v2(company_key),
    CONSTRAINT fk_fact_date_QA FOREIGN KEY (date_key) REFERENCES AIRFLOW0105.DEV.dim_date_QA_v2(date_key),
    CONSTRAINT uk_fact_stock_daily_QA_v2 UNIQUE (company_key, date_key)
);
"""

# -----------------------------------------------------------------------------
# Task 2: Load/Update Dimension Tables (using MERGE for incremental load)
# -----------------------------------------------------------------------------
SQL_MERGE_DIM_COMPANY = """
MERGE INTO AIRFLOW0105.DEV.dim_company_QA_v2 AS target
USING (
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
ON target.symbol = source.SYMBOL

WHEN MATCHED AND (
    COALESCE(target.company_name, '') != COALESCE(source.COMPANYNAME, '') OR
    COALESCE(target.industry, '') != COALESCE(source.INDUSTRY, '') OR
    COALESCE(target.sector, '') != COALESCE(source.SECTOR, '') OR
    COALESCE(target.exchange, '') != COALESCE(source.EXCHANGE, '') OR
    COALESCE(target.ceo, '') != COALESCE(source.CEO, '') OR
    COALESCE(target.mktcap, 0) != COALESCE(source.MKTCAP, 0) OR
    COALESCE(target.beta, 0) != COALESCE(source.BETA, 0) OR
    COALESCE(target.last_div, 0) != COALESCE(source.LASTDIV, 0) OR
    COALESCE(target.dcf, 0) != COALESCE(source.DCF, 0)
) THEN UPDATE SET
    target.company_name = source.COMPANYNAME,
    target.industry = source.INDUSTRY,
    target.sector = source.SECTOR,
    target.exchange = source.EXCHANGE,
    target.ceo = source.CEO,
    target.website = source.WEBSITE,
    target.description = source.DESCRIPTION,
    target.mktcap = source.MKTCAP,
    target.beta = source.BETA,
    target.last_div = source.LASTDIV,
    target.dcf = source.DCF,
    target.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
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
    dcf
) VALUES (
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
    source.DCF
);
"""

SQL_MERGE_DIM_DATE = """
MERGE INTO AIRFLOW0105.DEV.dim_date_QA_v2 AS target
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
# Task 3: Load/Update Fact Table (using MERGE for incremental load)
# -----------------------------------------------------------------------------
SQL_MERGE_FACT_STOCK = """
MERGE INTO AIRFLOW0105.DEV.fact_stock_daily_QA_v2 AS target
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
    INNER JOIN AIRFLOW0105.DEV.dim_company_QA_v2 dc 
        ON sh.SYMBOL = dc.symbol
    INNER JOIN AIRFLOW0105.DEV.dim_date_QA_v2 dd 
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
SELECT 'dim_company_QA_v2' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_company_QA_v2
UNION ALL
SELECT 'dim_date_QA_v2' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.dim_date_QA_v2
UNION ALL
SELECT 'fact_stock_daily_QA_v2' AS table_name, COUNT(*) AS row_count FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v2
UNION ALL
SELECT 'source_company_profile' AS table_name, COUNT(*) AS row_count FROM US_STOCK_DAILY.DCCM.Company_Profile
UNION ALL
SELECT 'source_stock_history' AS table_name, COUNT(*) AS row_count FROM US_STOCK_DAILY.DCCM.Stock_History;
"""

SQL_VALIDATE_REFERENTIAL_INTEGRITY = """
-- Validation: Check for orphan records in fact table
SELECT 
    'orphan_company_keys' AS check_name,
    COUNT(*) AS orphan_count
FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v2 f
LEFT JOIN AIRFLOW0105.DEV.dim_company_QA_v2 c ON f.company_key = c.company_key
WHERE c.company_key IS NULL

UNION ALL

SELECT 
    'orphan_date_keys' AS check_name,
    COUNT(*) AS orphan_count
FROM AIRFLOW0105.DEV.fact_stock_daily_QA_v2 f
LEFT JOIN AIRFLOW0105.DEV.dim_date_QA_v2 d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
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
load_dim_company = SnowflakeOperator(
    task_id='load_dim_company',
    snowflake_conn_id='jan_airflow_snowflake',
    sql=SQL_MERGE_DIM_COMPANY,
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

# =============================================================================
# Task Dependencies
# =============================================================================
# 
# DAG Flow :
#
#    create_dim_company ──┬──► create_fact_stock ──┐
#                         │                        │
#    create_dim_date ─────┘                        │
#            │                                     │
#            ▼                                     ▼
#    load_dim_company ────┬──► load_fact_stock ──► validate
#                         │
#    load_dim_date ───────┘
#

# Step 1: Create dimension tables first (can run in parallel)
[create_dim_company, create_dim_date] >> create_fact_stock

# Step 2: Load dimensions after their tables are created
create_dim_company >> load_dim_company
create_dim_date >> load_dim_date

# Step 3: Load fact table after BOTH:
#   - fact table is created
#   - dimension tables are loaded (need the keys for JOIN)
create_fact_stock >> load_fact_stock
[load_dim_company, load_dim_date] >> load_fact_stock

# Step 4: Validate after fact table is loaded
load_fact_stock >> validate_row_counts >> validate_referential_integrity
