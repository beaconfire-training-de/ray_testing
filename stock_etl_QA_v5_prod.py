"""
Stock Dimensional Model ETL - v5 Production Grade
Author: QA
Description:
TaskFlow + SnowflakeOperator + Clean Architecture
"""

from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.branch import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# =============================================================================
# CONFIG
# =============================================================================

SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"

TARGET_SCHEMA = "AIRFLOW0105.DEV"
SOURCE_SCHEMA = "US_STOCK_DAILY.DCCM"

METADATA_TABLE = f"{TARGET_SCHEMA}.etl_metadata_v5"
DIM_COMPANY_TABLE = f"{TARGET_SCHEMA}.dim_company_v5"
DIM_DATE_TABLE = f"{TARGET_SCHEMA}.dim_date_v5"
FACT_TABLE = f"{TARGET_SCHEMA}.fact_stock_daily_v5"

JOB_NAME = "stock_dimensional_model_QA_v5"

default_args = {
    "owner": "qa",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# =============================================================================
# DAG
# =============================================================================

@dag(
    dag_id="stock_dimensional_model_QA_v5",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["production", "taskflow", "snowflake", "v5"],
)
def stock_dimensional_model():

    # =============================================================================
    # 1️⃣ WATERMARK LOGIC (TaskFlow)
    # =============================================================================

    @task
    def get_run_mode():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        query = f"""
        SELECT watermark_date
        FROM {METADATA_TABLE}
        WHERE job_name = '{JOB_NAME}'
        AND run_status = 'success'
        ORDER BY run_end_time DESC
        LIMIT 1;
        """

        result = hook.get_first(query)
        today = datetime.now().date()

        if not result:
            return {"mode": "full", "watermark": None}

        if today.day == 1:
            return {"mode": "full", "watermark": None}

        return {"mode": "incremental", "watermark": result[0]}

    run_config = get_run_mode()

    # =============================================================================
    # 2️⃣ METADATA START LOG
    # =============================================================================

    log_start = SnowflakeOperator(
        task_id="log_start",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        INSERT INTO {METADATA_TABLE}
        (job_name, run_mode, run_status, run_start_time)
        VALUES ('{JOB_NAME}', '{{{{ ti.xcom_pull(task_ids="get_run_mode")["mode"] }}}}', 'running', CURRENT_TIMESTAMP());
        """,
    )

    # =============================================================================
    # 3️⃣ BRANCH
    # =============================================================================

    def choose_path(ti):
        config = ti.xcom_pull(task_ids="get_run_mode")
        return "full.start" if config["mode"] == "full" else "incremental.start"

    branch = BranchPythonOperator(
        task_id="branch_mode",
        python_callable=choose_path,
    )

    # =============================================================================
    # 4️⃣ FULL LOAD GROUP
    # =============================================================================

    with TaskGroup("full") as full_group:

        start = EmptyOperator(task_id="start")

        full_dim_date = SnowflakeOperator(
            task_id="load_dim_date",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
            MERGE INTO {DIM_DATE_TABLE} t
            USING (
                SELECT DISTINCT
                    TO_NUMBER(TO_CHAR(DATE,'YYYYMMDD')) date_key,
                    DATE full_date
                FROM {SOURCE_SCHEMA}.Stock_History
            ) s
            ON t.date_key = s.date_key
            WHEN NOT MATCHED THEN
            INSERT (date_key, full_date)
            VALUES (s.date_key, s.full_date);
            """,
        )

        full_fact = SnowflakeOperator(
            task_id="load_fact",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
            MERGE INTO {FACT_TABLE} t
            USING (
                SELECT
                    sh.SYMBOL,
                    sh.DATE,
                    sh.OPEN,
                    sh.CLOSE
                FROM {SOURCE_SCHEMA}.Stock_History sh
            ) s
            ON t.date_key = TO_NUMBER(TO_CHAR(s.DATE,'YYYYMMDD'))
            WHEN NOT MATCHED THEN
            INSERT (date_key, open_price, close_price, load_date)
            VALUES (
                TO_NUMBER(TO_CHAR(s.DATE,'YYYYMMDD')),
                s.OPEN,
                s.CLOSE,
                CURRENT_DATE()
            );
            """,
        )

        start >> full_dim_date >> full_fact

    # =============================================================================
    # 5️⃣ INCREMENTAL GROUP
    # =============================================================================

    with TaskGroup("incremental") as incr_group:

        start = EmptyOperator(task_id="start")

        incr_fact = SnowflakeOperator(
            task_id="load_fact",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
            MERGE INTO {FACT_TABLE} t
            USING (
                SELECT *
                FROM {SOURCE_SCHEMA}.Stock_History
                WHERE DATE > '{{{{ ti.xcom_pull(task_ids="get_run_mode")["watermark"] }}}}'
            ) s
            ON t.date_key = TO_NUMBER(TO_CHAR(s.DATE,'YYYYMMDD'))
            WHEN NOT MATCHED THEN
            INSERT (date_key, open_price, close_price, load_date)
            VALUES (
                TO_NUMBER(TO_CHAR(s.DATE,'YYYYMMDD')),
                s.OPEN,
                s.CLOSE,
                CURRENT_DATE()
            );
            """,
        )

        start >> incr_fact

    # =============================================================================
    # 6️⃣ JOIN
    # =============================================================================

    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =============================================================================
    # 7️⃣ METADATA SUCCESS
    # =============================================================================

    log_success = SnowflakeOperator(
        task_id="log_success",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        sql=f"""
        UPDATE {METADATA_TABLE}
        SET run_status='success',
            run_end_time=CURRENT_TIMESTAMP(),
            watermark_date = (SELECT MAX(full_date) FROM {DIM_DATE_TABLE})
        WHERE metadata_id = (
            SELECT MAX(metadata_id)
            FROM {METADATA_TABLE}
            WHERE job_name='{JOB_NAME}'
            AND run_status='running'
        );
        """,
    )

    # =============================================================================
    # DEPENDENCIES
    # =============================================================================

    run_config >> log_start >> branch
    branch >> full_group >> join
    branch >> incr_group >> join
    join >> log_success


dag = stock_dimensional_model()
