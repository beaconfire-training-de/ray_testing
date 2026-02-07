"""
Stock Dimensional Model ETL Pipeline - Version 4 (Production)
==============================================================

v4: Production-grade with incremental loading (CURRENT VERSION)
    - Implemented watermark-based incremental loading
    - Added full vs incremental mode with dynamic branching
    - Created metadata tracking table for ETL run history
    - Performance optimization: only process new data daily
    - Added comprehensive data quality checks
    - Added load_date column for partition pruning
    - Improved error handling and monitoring

FEATURES IN v4:
---------------
1. Watermark Mechanism: Track last processed date, avoid reprocessing
2. Dynamic Mode Selection: Auto-switch between full and incremental
3. Metadata Tracking: Log every run with status, rows processed, timing
4. Incremental Loading: Process only new data since last watermark
5. Monthly Full Refresh: Auto full load on 1st of each month
6. Data Quality Checks: Validate row counts, nulls, referential integrity
7. Performance Optimization: Partition pruning with load_date column

TABLES:
-------
- etl_metadata: Tracks ETL run history and watermarks
- dim_company: Company dimension with SCD Type 2
- dim_date: Date dimension
- fact_stock_daily: Stock price fact table

"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    'owner': 'qa',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,  # Set to True and configure email in production
    'email': ['blessanq@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_dimensional_model_QA_v4_prod',
    default_args=default_args,
    description='Production ETL pipeline v4 - Incremental loading with watermark',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['v4', 'production', 'stock', 'dimensional_model', 'scd2', 'incremental'],
    max_active_runs=1,  # Prevent concurrent runs
)

# =============================================================================
# Configuration Variables
# =============================================================================
SNOWFLAKE_CONN_ID = 'jan_airflow_snowflake'
TARGET_SCHEMA = 'AIRFLOW0105.DEV'
SOURCE_SCHEMA = 'US_STOCK_DAILY.DCCM'
METADATA_TABLE = f'{TARGET_SCHEMA}.etl_metadata_v4'
JOB_NAME = 'stock_dimensional_model_QA_v4_prod'

DIM_COMPANY_TABLE = f'{TARGET_SCHEMA}.dim_company_v4'
DIM_DATE_TABLE = f'{TARGET_SCHEMA}.dim_date_v4'
FACT_STOCK_TABLE = f'{TARGET_SCHEMA}.fact_stock_daily_v4'

# =============================================================================
# SQL: Create Metadata Table (NEW in v4)
# =============================================================================
SQL_CREATE_METADATA_TABLE = """
-- v4: Metadata table to track ETL runs and watermarks
CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
    metadata_id INT AUTOINCREMENT PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    watermark_date DATE,
    run_mode VARCHAR(20) NOT NULL,  -- 'full' or 'incremental'
    rows_processed INT,
    run_status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed'
    run_start_time TIMESTAMP_NTZ,
    run_end_time TIMESTAMP_NTZ,
    error_message VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT chk_run_mode CHECK (run_mode IN ('full', 'incremental')),
    CONSTRAINT chk_run_status CHECK (run_status IN ('running', 'success', 'failed'))
);

-- Create index for faster watermark lookups
CREATE INDEX IF NOT EXISTS idx_metadata_job_status 
ON {METADATA_TABLE}(job_name, run_status, run_end_time);
"""

# =============================================================================
# SQL: Create Dimension and Fact Tables
# =============================================================================
SQL_CREATE_DIM_COMPANY = """
-- v4: Company dimension with SCD Type 2 (inherited from v3)
-- v4 addition: Added updated_at for better tracking
CREATE TABLE IF NOT EXISTS {DIM_COMPANY_TABLE} (
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
    
    -- SCD Type 2 fields (from v3)
    effective_date DATE DEFAULT CURRENT_DATE(),
    expiration_date DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT uk_dim_company_symbol_effective_v4 UNIQUE (symbol, effective_date)
);

-- v4: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_company_symbol_current_v4 
ON {DIM_COMPANY_TABLE}(symbol, is_current);

CREATE INDEX IF NOT EXISTS idx_company_current_v4 
ON {DIM_COMPANY_TABLE}(is_current);
"""

SQL_CREATE_DIM_DATE = """
-- Date dimension (stable from v1, no changes in v4)
CREATE TABLE IF NOT EXISTS {DIM_DATE_TABLE} (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week VARCHAR(10),
    day_of_week_num INT,
    is_weekend BOOLEAN,
    
    CONSTRAINT uk_dim_date_fulldate_v4 UNIQUE (full_date)
);

-- v4: Create index on full_date for faster joins
CREATE INDEX IF NOT EXISTS idx_date_fulldate_v4 
ON {DIM_DATE_TABLE}(full_date);
"""

SQL_CREATE_FACT_STOCK = """
-- v4: Fact table with load_date for partition pruning (NEW in v4)
CREATE TABLE IF NOT EXISTS {FACT_STOCK_TABLE} (
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
    
    -- v4: Added load_date for incremental loading tracking
    load_date DATE DEFAULT CURRENT_DATE(),
    
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT fk_fact_company_v4 
        FOREIGN KEY (company_key) REFERENCES {DIM_COMPANY_TABLE}(company_key),
    CONSTRAINT fk_fact_date_v4 
        FOREIGN KEY (date_key) REFERENCES {DIM_DATE_TABLE}(date_key),
    CONSTRAINT uk_fact_stock_daily_v4 
        UNIQUE (company_key, date_key)
);

-- v4: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_load_date_v4 
ON {FACT_STOCK_TABLE}(load_date);

CREATE INDEX IF NOT EXISTS idx_fact_company_date_v4 
ON {FACT_STOCK_TABLE}(company_key, date_key);
"""

# =============================================================================
# Python Functions for Watermark and Branching (NEW in v4)
# =============================================================================

def log_etl_start(**context):
    """
    v4: Log ETL job start in metadata table
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Get run mode from XCom (will be set by get_watermark_and_mode)
    ti = context['task_instance']
    run_mode = ti.xcom_pull(task_ids='get_watermark_and_mode', key='run_mode')
    
    if not run_mode:
        run_mode = 'full'  # Default to full if not set
    
    insert_query = """
    INSERT INTO {METADATA_TABLE} (
        job_name, run_mode, run_status, run_start_time
    ) VALUES (
        '{JOB_NAME}', '{run_mode}', 'running', CURRENT_TIMESTAMP()
    );
    """
    
    hook.run(insert_query)
    print(f"Logged ETL start for job: {JOB_NAME}, mode: {run_mode}")


def get_watermark_and_mode(**context):
    """
    v4: Get last watermark and determine run mode
    Logic:
    - First run: Full load
    - 1st of month: Full load (monthly refresh)
    - Other days: Incremental load from last watermark
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Get last successful watermark
    query = """
    SELECT watermark_date, run_mode, run_end_time
    FROM {METADATA_TABLE}
    WHERE job_name = '{JOB_NAME}'
    AND run_status = 'success'
    ORDER BY run_end_time DESC
    LIMIT 1;
    """
    
    try:
        result = hook.get_first(query)
        current_date = datetime.now().date()
        
        if result:
            last_watermark = result[0]
            last_mode = result[1]
            last_run_time = result[2]
            
            print(f"Last successful run: {last_run_time}")
            print(f"Last watermark: {last_watermark}, Last mode: {last_mode}")
            
            # Determine mode: Full refresh on 1st of month
            if current_date.day == 1:
                mode = 'full'
                watermark = None
                print("Monthly full refresh triggered (1st of month)")
            else:
                mode = 'incremental'
                watermark = last_watermark
                print(f"Incremental mode: processing data after {watermark}")
                
        else:
            # First run - do full load
            mode = 'full'
            watermark = None
            print("First run detected - performing full load")
            
    except Exception as e:
        print(f"Error getting watermark: {e}")
        print("Defaulting to full load for safety")
        mode = 'full'
        watermark = None
    
    # Push to XCom for downstream tasks
    ti = context['task_instance']
    ti.xcom_push(key='run_mode', value=mode)
    ti.xcom_push(key='watermark_date', value=str(watermark) if watermark else None)
    
    print(f"Final decision - Mode: {mode}, Watermark: {watermark}")
    
    return {'mode': mode, 'watermark': watermark}


def branch_on_mode(**context):
    """
    v4: Branch based on run mode
    Returns task_id of next task to execute
    """
    ti = context['task_instance']
    mode = ti.xcom_pull(task_ids='get_watermark_and_mode', key='run_mode')
    
    print(f"Branching decision: mode = {mode}")
    
    if mode == 'full':
        print("Taking FULL LOAD path")
        return 'start_full_load'
    else:
        print("Taking INCREMENTAL LOAD path")
        return 'start_incremental_load'


def log_etl_success(**context):
    """
    v4: Log successful ETL completion in metadata table
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    ti = context['task_instance']
    
    # Get the latest date from dim_date as new watermark
    watermark_query = f"SELECT MAX(full_date) FROM {DIM_DATE_TABLE};"
    new_watermark = hook.get_first(watermark_query)[0]
    
    # Get row count
    count_query = """
    SELECT COUNT(*) FROM {FACT_STOCK_TABLE} 
    WHERE load_date = CURRENT_DATE();
    """
    rows_processed = hook.get_first(count_query)[0]
    
    # Update metadata
    update_query = """
    UPDATE {METADATA_TABLE}
    SET 
        watermark_date = '{new_watermark}',
        rows_processed = {rows_processed},
        run_status = 'success',
        run_end_time = CURRENT_TIMESTAMP()
    WHERE job_name = '{JOB_NAME}'
    AND run_status = 'running'
    AND metadata_id = (
        SELECT MAX(metadata_id) 
        FROM {METADATA_TABLE} 
        WHERE job_name = '{JOB_NAME}' 
        AND run_status = 'running'
    );
    """
    
    hook.run(update_query)
    
    print(f"ETL completed successfully!")
    print(f"New watermark: {new_watermark}")
    print(f"Rows processed: {rows_processed}")


def log_etl_failure(**context):
    """
    v4: Log ETL failure in metadata table
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Get exception info if available
    exception = context.get('exception', 'Unknown error')
    
    update_query = """
    UPDATE {METADATA_TABLE}
    SET 
        run_status = 'failed',
        run_end_time = CURRENT_TIMESTAMP(),
        error_message = '{str(exception)[:1000]}'
    WHERE job_name = '{JOB_NAME}'
    AND run_status = 'running'
    AND metadata_id = (
        SELECT MAX(metadata_id) 
        FROM {METADATA_TABLE} 
        WHERE job_name = '{JOB_NAME}' 
        AND run_status = 'running'
    );
    """
    
    try:
        hook.run(update_query)
        print(f"ETL failed. Error logged: {exception}")
    except Exception as e:
        print(f"Failed to log error: {e}")


# =============================================================================
# SQL: Incremental Load Queries (NEW in v4)
# =============================================================================

def get_incremental_dim_date_sql(watermark_date):
    """Generate SQL for incremental dim_date load"""
    watermark_filter = f"AND DATE > '{watermark_date}'" if watermark_date else ""
    
    return """
    MERGE INTO {DIM_DATE_TABLE} AS target
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
        FROM {SOURCE_SCHEMA}.Stock_History
        WHERE DATE IS NOT NULL
        {watermark_filter}
    ) AS source
    ON target.date_key = source.date_key
    WHEN NOT MATCHED THEN INSERT (
        date_key, full_date, year, month, day, quarter, 
        day_of_week, day_of_week_num, is_weekend
    ) VALUES (
        source.date_key, source.full_date, source.year, source.month, 
        source.day, source.quarter, source.day_of_week, 
        source.day_of_week_num, source.is_weekend
    );
    """

SQL_INCREMENTAL_SCD2_EXPIRE = """
-- v4: SCD2 - Expire old records for companies with changes (from v3)
UPDATE {DIM_COMPANY_TABLE} AS target
SET 
    expiration_date = CURRENT_DATE(),
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP()
WHERE target.is_current = TRUE
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
        FROM {SOURCE_SCHEMA}.Symbols s
        LEFT JOIN {SOURCE_SCHEMA}.Company_Profile cp ON s.SYMBOL = cp.SYMBOL
        WHERE s.SYMBOL IS NOT NULL
    ) AS source
    WHERE source.SYMBOL = target.symbol
    AND (
        -- Track changes in these fields (SCD Type 2)
        COALESCE(target.company_name, '') != COALESCE(source.COMPANYNAME, '') OR
        COALESCE(target.industry, '') != COALESCE(source.INDUSTRY, '') OR
        COALESCE(target.sector, '') != COALESCE(source.SECTOR, '') OR
        COALESCE(target.exchange, '') != COALESCE(source.EXCHANGE, '') OR
        COALESCE(target.ceo, '') != COALESCE(source.CEO, '')
    )
);
"""

SQL_INCREMENTAL_SCD2_INSERT = """
-- v4: SCD2 - Insert new versions (from v3)
INSERT INTO {DIM_COMPANY_TABLE} (
    symbol, company_name, industry, sector, exchange, ceo,
    website, description, mktcap, beta, last_div, dcf,
    effective_date, expiration_date, is_current
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
    TO_DATE('9999-12-31', 'YYYY-MM-DD'),
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
    FROM {SOURCE_SCHEMA}.Symbols s
    LEFT JOIN {SOURCE_SCHEMA}.Company_Profile cp ON s.SYMBOL = cp.SYMBOL
    WHERE s.SYMBOL IS NOT NULL
) AS source
WHERE NOT EXISTS (
    SELECT 1 
    FROM {DIM_COMPANY_TABLE} AS target
    WHERE target.symbol = source.SYMBOL
    AND target.is_current = TRUE
);
"""

SQL_INCREMENTAL_SCD1_UPDATE = """
-- v4: SCD1 - Update non-tracked fields (from v3)
UPDATE {DIM_COMPANY_TABLE} AS target
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
    FROM {SOURCE_SCHEMA}.Symbols s
    LEFT JOIN {SOURCE_SCHEMA}.Company_Profile cp ON s.SYMBOL = cp.SYMBOL
    WHERE s.SYMBOL IS NOT NULL
) AS source
WHERE target.symbol = source.SYMBOL
AND target.is_current = TRUE;
"""
def get_incremental_fact_load_sql(watermark_date):
    """Generate SQL for incremental fact load"""
    watermark_filter = f"AND sh.DATE > '{watermark_date}'" if watermark_date else ""
    
    return """
    MERGE INTO {FACT_STOCK_TABLE} AS target
    USING (
        SELECT 
            company_key, date_key, open_price, high_price, low_price,
            close_price, adj_close, volume, price_change
        FROM (
            SELECT
                dc.company_key,
                dd.date_key,
                sh.OPEN AS open_price,
                sh.HIGH AS high_price,
                sh.LOW AS low_price,
                sh.CLOSE AS close_price,
                sh.ADJCLOSE AS adj_close,
                sh.VOLUME AS volume,
                (sh.CLOSE - sh.OPEN) AS price_change,
                ROW_NUMBER() OVER (
                    PARTITION BY dc.company_key, dd.date_key 
                    ORDER BY sh.DATE DESC
                ) AS rn
            FROM {SOURCE_SCHEMA}.Stock_History sh
            INNER JOIN {DIM_COMPANY_TABLE} dc 
                ON sh.SYMBOL = dc.symbol AND dc.is_current = TRUE
            INNER JOIN {DIM_DATE_TABLE} dd 
                ON sh.DATE = dd.full_date
            WHERE sh.SYMBOL IS NOT NULL AND sh.DATE IS NOT NULL
            {watermark_filter}
        ) deduped
        WHERE rn = 1
    ) AS source
    ON target.company_key = source.company_key 
       AND target.date_key = source.date_key
    WHEN MATCHED THEN UPDATE SET
        target.open_price = source.open_price,
        target.high_price = source.high_price,
        target.low_price = source.low_price,
        target.close_price = source.close_price,
        target.adj_close = source.adj_close,
        target.volume = source.volume,
        target.price_change = source.price_change,
        target.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        company_key, date_key, open_price, high_price, low_price,
        close_price, adj_close, volume, price_change, load_date
    ) VALUES (
        source.company_key, source.date_key, source.open_price, 
        source.high_price, source.low_price, source.close_price, 
        source.adj_close, source.volume, source.price_change, CURRENT_DATE()
    );
    """
def run_incremental_dim_date(**context):
    """Execute incremental load for dim_date"""
    ti = context['task_instance']
    watermark_date = ti.xcom_pull(task_ids='get_watermark_and_mode', key='watermark_date')
    
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = get_incremental_dim_date_sql(watermark_date)
    
    print(f"Executing incremental dim_date load with watermark: {watermark_date}")
    hook.run(sql)
    print("Incremental dim_date load completed")


def run_incremental_fact_load(**context):
    """Execute incremental load for fact table"""
    ti = context['task_instance']
    watermark_date = ti.xcom_pull(task_ids='get_watermark_and_mode', key='watermark_date')
    
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = get_incremental_fact_load_sql(watermark_date)
    
    print(f"Executing incremental fact load with watermark: {watermark_date}")
    hook.run(sql)
    print("Incremental fact load completed")

# =============================================================================
# SQL: Full Load Queries (v4 - same as incremental but without watermark filter)
# =============================================================================

def run_full_dim_date(**context):
    """Execute full load for dim_date"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = get_incremental_dim_date_sql(None)  # No watermark = full load
    
    print("Executing full dim_date load")
    hook.run(sql)
    print("Full dim_date load completed")


def run_full_fact_load(**context):
    """Execute full load for fact table"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = get_incremental_fact_load_sql(None)  # No watermark = full load
    
    print("Executing full fact load")
    hook.run(sql)
    print("Full fact load completed")

# =============================================================================
# SQL: Data Quality Checks (NEW in v4)
# =============================================================================

SQL_DQ_ROW_COUNTS = """
-- v4: Validate row counts
SELECT 
    'dim_company_total' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {DIM_COMPANY_TABLE}

UNION ALL

SELECT 
    'dim_company_current' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {DIM_COMPANY_TABLE} 
WHERE is_current = TRUE

UNION ALL

SELECT 
    'dim_company_historical' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {DIM_COMPANY_TABLE} 
WHERE is_current = FALSE

UNION ALL

SELECT 
    'dim_date_total' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {DIM_DATE_TABLE}

UNION ALL

SELECT 
    'fact_stock_total' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {FACT_STOCK_TABLE}

UNION ALL

SELECT 
    'fact_stock_today' AS metric_name,
    COUNT(*) AS metric_value,
    CURRENT_TIMESTAMP() AS check_time
FROM {FACT_STOCK_TABLE} 
WHERE load_date = CURRENT_DATE();
"""

SQL_DQ_NULL_CHECK = """
-- v4: Check for null values in critical fields
SELECT 
    'null_company_key' AS check_name,
    COUNT(*) AS issue_count,
    'HIGH' AS severity
FROM {FACT_STOCK_TABLE}
WHERE company_key IS NULL 
AND load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'null_date_key' AS check_name,
    COUNT(*) AS issue_count,
    'HIGH' AS severity
FROM {FACT_STOCK_TABLE}
WHERE date_key IS NULL 
AND load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'null_prices' AS check_name,
    COUNT(*) AS issue_count,
    'MEDIUM' AS severity
FROM {FACT_STOCK_TABLE}
WHERE (open_price IS NULL OR close_price IS NULL) 
AND load_date = CURRENT_DATE();
"""

SQL_DQ_REFERENTIAL_INTEGRITY = """
-- v4: Check referential integrity
SELECT 
    'orphan_company_keys' AS check_name,
    COUNT(*) AS orphan_count,
    'HIGH' AS severity
FROM {FACT_STOCK_TABLE} f
LEFT JOIN {DIM_COMPANY_TABLE} c ON f.company_key = c.company_key
WHERE c.company_key IS NULL 
AND f.load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'orphan_date_keys' AS check_name,
    COUNT(*) AS orphan_count,
    'HIGH' AS severity
FROM {FACT_STOCK_TABLE} f
LEFT JOIN {DIM_DATE_TABLE} d ON f.date_key = d.date_key
WHERE d.date_key IS NULL 
AND f.load_date = CURRENT_DATE();
"""

SQL_DQ_SCD2_INTEGRITY = """
-- v4: Validate SCD Type 2 integrity
SELECT 
    'symbols_with_multiple_current' AS check_name,
    COUNT(*) AS issue_count,
    'HIGH' AS severity
FROM (
    SELECT symbol, COUNT(*) as cnt
    FROM {DIM_COMPANY_TABLE}
    WHERE is_current = TRUE
    GROUP BY symbol
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT 
    'symbols_with_no_current' AS check_name,
    COUNT(*) AS issue_count,
    'MEDIUM' AS severity
FROM (
    SELECT DISTINCT symbol 
    FROM {DIM_COMPANY_TABLE}
    WHERE symbol NOT IN (
        SELECT symbol 
        FROM {DIM_COMPANY_TABLE} 
        WHERE is_current = TRUE
    )
);
"""

SQL_DQ_BUSINESS_RULES = """
-- v4: Validate business rules
SELECT 
    'negative_prices' AS check_name,
    COUNT(*) AS issue_count,
    'HIGH' AS severity
FROM {FACT_STOCK_TABLE}
WHERE (open_price < 0 OR close_price < 0 OR high_price < 0 OR low_price < 0)
AND load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'negative_volume' AS check_name,
    COUNT(*) AS issue_count,
    'MEDIUM' AS severity
FROM {FACT_STOCK_TABLE}
WHERE volume < 0
AND load_date = CURRENT_DATE()

UNION ALL

SELECT 
    'unrealistic_prices' AS check_name,
    COUNT(*) AS issue_count,
    'MEDIUM' AS severity
FROM {FACT_STOCK_TABLE}
WHERE (open_price > 100000 OR close_price > 100000)
AND load_date = CURRENT_DATE();
"""

# =============================================================================
# Airflow Tasks
# =============================================================================

# Task 0: Setup - Create tables if not exist
create_metadata_table = SnowflakeOperator(
    task_id='create_metadata_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_CREATE_METADATA_TABLE,
    dag=dag,
)

create_dim_company = SnowflakeOperator(
    task_id='create_dim_company',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_CREATE_DIM_COMPANY,
    dag=dag,
)

create_dim_date = SnowflakeOperator(
    task_id='create_dim_date',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_CREATE_DIM_DATE,
    dag=dag,
)

create_fact_stock = SnowflakeOperator(
    task_id='create_fact_stock',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_CREATE_FACT_STOCK,
    dag=dag,
)

# Task 1: Get watermark and determine mode
get_watermark_task = PythonOperator(
    task_id='get_watermark_and_mode',
    python_callable=get_watermark_and_mode,
    provide_context=True,
    dag=dag,
)

# Task 2: Log ETL start
log_start = PythonOperator(
    task_id='log_etl_start',
    python_callable=log_etl_start,
    provide_context=True,
    dag=dag,
)

# Task 3: Branch based on mode
branch_mode = BranchPythonOperator(
    task_id='branch_mode',
    python_callable=branch_on_mode,
    provide_context=True,
    dag=dag,
)

# ========== FULL LOAD PATH ==========
start_full_load = EmptyOperator(
    task_id='start_full_load',
    dag=dag,
)

full_load_dim_date = PythonOperator(
    task_id='full_load_dim_date',
    python_callable=run_full_dim_date,
    provide_context=True,
    dag=dag,
)

full_scd2_expire = SnowflakeOperator(
    task_id='full_scd2_expire',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD2_EXPIRE,
    dag=dag,
)

full_scd2_insert = SnowflakeOperator(
    task_id='full_scd2_insert',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD2_INSERT,
    dag=dag,
)

full_scd1_update = SnowflakeOperator(
    task_id='full_scd1_update',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD1_UPDATE,
    dag=dag,
)

full_load_fact = PythonOperator(
    task_id='full_load_fact',
    python_callable=run_full_fact_load,
    provide_context=True,
    dag=dag,
)
# ========== INCREMENTAL LOAD PATH ==========
start_incremental_load = EmptyOperator(
    task_id='start_incremental_load',
    dag=dag,
)

incr_load_dim_date = PythonOperator(
    task_id='incr_load_dim_date',
    python_callable=run_incremental_dim_date,
    provide_context=True,
    dag=dag,
)

incr_scd2_expire = SnowflakeOperator(
    task_id='incr_scd2_expire',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD2_EXPIRE,
    dag=dag,
)

incr_scd2_insert = SnowflakeOperator(
    task_id='incr_scd2_insert',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD2_INSERT,
    dag=dag,
)

incr_scd1_update = SnowflakeOperator(
    task_id='incr_scd1_update',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INCREMENTAL_SCD1_UPDATE,
    dag=dag,
)

incr_load_fact = PythonOperator(
    task_id='incr_load_fact',
    python_callable=run_incremental_fact_load,
    provide_context=True,
    dag=dag,
)

# ========== JOIN POINT ==========
join_paths = EmptyOperator(
    task_id='join_paths',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# ========== DATA QUALITY CHECKS ==========
dq_row_counts = SnowflakeOperator(
    task_id='dq_row_counts',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_DQ_ROW_COUNTS,
    dag=dag,
)

dq_null_check = SnowflakeOperator(
    task_id='dq_null_check',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_DQ_NULL_CHECK,
    dag=dag,
)

dq_referential_integrity = SnowflakeOperator(
    task_id='dq_referential_integrity',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_DQ_REFERENTIAL_INTEGRITY,
    dag=dag,
)

dq_scd2_integrity = SnowflakeOperator(
    task_id='dq_scd2_integrity',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_DQ_SCD2_INTEGRITY,
    dag=dag,
)

dq_business_rules = SnowflakeOperator(
    task_id='dq_business_rules',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_DQ_BUSINESS_RULES,
    dag=dag,
)

# ========== FINALIZATION ==========
log_success = PythonOperator(
    task_id='log_etl_success',
    python_callable=log_etl_success,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

log_failure = PythonOperator(
    task_id='log_etl_failure',
    python_callable=log_etl_failure,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================

# Setup phase
create_metadata_table >> [create_dim_company, create_dim_date, create_fact_stock]
[create_dim_company, create_dim_date, create_fact_stock] >> get_watermark_task
get_watermark_task >> log_start >> branch_mode

# Full load path
branch_mode >> start_full_load
start_full_load >> [full_load_dim_date, full_scd2_expire]
full_scd2_expire >> full_scd2_insert >> full_scd1_update
[full_load_dim_date, full_scd1_update] >> full_load_fact >> join_paths

# Incremental load path
branch_mode >> start_incremental_load
start_incremental_load >> [incr_load_dim_date, incr_scd2_expire]
incr_scd2_expire >> incr_scd2_insert >> incr_scd1_update
[incr_load_dim_date, incr_scd1_update] >> incr_load_fact >> join_paths

# Data quality checks
join_paths >> [dq_row_counts, dq_null_check, dq_referential_integrity, dq_scd2_integrity, dq_business_rules]

# Finalization
[dq_row_counts, dq_null_check, dq_referential_integrity, dq_scd2_integrity, dq_business_rules] >> log_success
[dq_row_counts, dq_null_check, dq_referential_integrity, dq_scd2_integrity, dq_business_rules] >> log_failure
[log_success, log_failure] >> end
