from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount
from spark_utils import build_spark_submit
from datetime import datetime, timedelta, timezone
import logging
import sys
import os

# Get current UTC datetime
now_utc = datetime.now(timezone.utc)

# Current date in YYYY-MM-DD format
snapshot_date = now_utc.date().isoformat()  # e.g., "2026-03-02"

# Current UTC timestamp in YYYY-MM-DD HH:MM:SS format
ingested_at_timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S")  # e.g., "2026-03-02 14:23:45"

# Add extract src dir to path

##################################################
# CALLBACKS
##################################################

def on_failure_callback(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    logging.error(
        "Task failed: dag_id=%s, task_id=%s, execution_date=%s",
        dag_id,
        task_id,
        execution_date,
    )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

MAX_DAG_RETRIES = 3
RETRY_COUNTER_VAR = "DAILY_SNAPSHOT_RETRY_COUNT"

def should_rerun_dag(**context):
    retries = int(Variable.get(RETRY_COUNTER_VAR, default_var=0))

    if retries >= MAX_DAG_RETRIES:
        raise AirflowSkipException("Max DAG retries reached")

    Variable.set(RETRY_COUNTER_VAR, retries + 1)

def reset_dag_retry_counter(**context):
    Variable.set(RETRY_COUNTER_VAR, 0)

##################################################
# FEATURE FLAG
##################################################

GLOBAL_EXTRACT_FLAG = "ENABLE_EXTRACT_STAGE"

##################################################
# FEATURE FLAG HELPER
##################################################

def skip_if_disabled(flag_name: str):
    enabled = Variable.get(flag_name, "true").lower() == "true"
    if not enabled:
        raise AirflowSkipException(f"{flag_name} disabled")

##################################################
# TASK CALLABLE
##################################################

def run_extract(raw_file: str):
    # One global switch for ALL extract tasks
    skip_if_disabled(GLOBAL_EXTRACT_FLAG)

    import extract_to_s3
    extract_to_s3.extract(raw_file)

##################################################
# DBT OPERATOR HELPER
##################################################

projects_dir = os.environ["PROJECTS_DIR"]

def make_dbt_operator(task_id: str, dbt_command: str) -> DockerOperator:
    """Return a configured DockerOperator for a dbt command."""
    return DockerOperator(
        task_id=task_id,
        image="dbt-spark:f5bf2ec",
        command=dbt_command,
        mounts=[
            Mount(
                source=f"{projects_dir}/seventh_art_analytics/transform",
                target="/usr/app/dbt",
                type="bind",
            )
        ],
        network_mode="seventh_art_analytics_iceberg_net",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        tty=True,
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=45),
    )

##################################################
# DAG
##################################################

with DAG(
    "daily_prod_etl_medallion",
    default_args=default_args,
    description="Extract to S3 -> Load to Iceberg (parallel Spark jobs)",
    schedule=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["production", "medallion", "imdb"],
    doc_md="ETL pipeline for IMDB data: extract → load (Iceberg) → transform (dbt)",
) as dag:

    ##################################################
    # Step 1: Extract all IMDB raw files to S3/MinIO #
    ##################################################
    EXTRACT_SCRIPT_DIR = "/opt/airflow/extract/src/"
    extract_raw_files = [
        "name.basics.tsv.gz",
        "title.principals.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.ratings.tsv.gz",
    ]

    extract_tasks = []
    sys.path.append(EXTRACT_SCRIPT_DIR)

    for raw_file in extract_raw_files:
        task_id = "extract_" + raw_file.replace(".", "_").replace("_tsv_gz", "_to_s3")
        extract_task = PythonOperator(
            task_id=task_id,
            python_callable=run_extract,
            op_kwargs={"raw_file": raw_file},
            execution_timeout=timedelta(minutes=30),
        )
        extract_tasks.append(extract_task)

    ########################################################################
    # Step 2: Load each raw file from S3/MinIO to Iceberg using Spark jobs #
    ########################################################################
    SPARK_JOBS_DIR = "/opt/airflow/load/src/"
    load_raw_jobs = [
        "create_tables",
        "load_to_iceberg_name_basics",
        "load_to_iceberg_title_akas",
        "load_to_iceberg_title_basics",
        "load_to_iceberg_title_crew",
        "load_to_iceberg_title_episode",
        "load_to_iceberg_title_principals",
        "load_to_iceberg_title_ratings"
        ]
    spark_raw_tasks = []

    snapshot_try = int(Variable.get(RETRY_COUNTER_VAR, default_var=0))

    for job in load_raw_jobs:
        spark_task_id = f"load_SPARK_stage_raw_{job}"
        spark_task = BashOperator(
            retries=2,
            retry_delay=timedelta(minutes=5),
            task_id=spark_task_id,
            bash_command=build_spark_submit(f"{SPARK_JOBS_DIR}{job}.py", snapshot_date, ingested_at_timestamp, snapshot_try),
            execution_timeout=timedelta(hours=2),
        )
        spark_raw_tasks.append(spark_task)

    ############################################
    # Step 3: Install DBT dependencies #
    ############################################
    dbt_deps_task = make_dbt_operator(
        task_id="transform_DBT_deps",
        dbt_command="""deps
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 4: Install DBT seed #
    ############################################
    dbt_seed_task = make_dbt_operator(
        task_id="transform_DBT_stage_seed",
        dbt_command="""seed
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 5: Transform Medallion Canonical layer #
    ############################################
    dbt_canonical_run_task = make_dbt_operator(
        task_id="transform_DBT_stage_canonical_layer",
        dbt_command="""run
    --profiles-dir /usr/app/dbt
    --models stage.canonical
    --target stage_canonical
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 6: Data Validation Canonical layer #
    ############################################
    dbt_canonical_validation_task = make_dbt_operator(
        task_id="transform_DBT_data_quality_check_stage_canonical_layer",
        dbt_command="""test
    --profiles-dir /usr/app/dbt
    --models stage.canonical
    --target stage_canonical
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 7: Transform Medallion Analytics layer #
    ############################################
    dbt_analytics_run_task = make_dbt_operator(
        task_id="transform_DBT_stage_analytics_layer",
        dbt_command="""run
    --profiles-dir /usr/app/dbt
    --models stage.analytics
    --target stage_analytics
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 8: Data Validation Analytics layer #
    ############################################
    dbt_analytics_validation_task = make_dbt_operator(
        task_id="transform_DBT_data_quality_check_stage_analytics_layer",
        dbt_command="""test
    --profiles-dir /usr/app/dbt
    --models stage.analytics
    --target stage_analytics
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 9: Promote to prod (schema swap via dbt macros)
    ############################################
    dbt_promote_canonical_task = make_dbt_operator(
        task_id="promote_DBT_stage_canonical_to_prod",
        dbt_command="""run-operation promote_canonical_to_prod
    --profiles-dir /usr/app/dbt
    --target stage_canonical
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    dbt_promote_analytics_task = make_dbt_operator(
        task_id="promote_DBT_stage_analytics_to_prod",
        dbt_command="""run-operation promote_analytics_to_prod
    --profiles-dir /usr/app/dbt
    --target stage_analytics
    --project-dir /usr/app/dbt/data_platform
    """,
    )

    ############################################
    # Step 10: Retry whole DAG on validation failure
    ############################################

    wait_30_minutes = TimeDeltaSensor(
        task_id="wait_30_minutes_before_dag_retry",
        delta=timedelta(minutes=30),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_retry_limit = PythonOperator(
        task_id="check_dag_retry_limit",
        python_callable=should_rerun_dag,
    )

    restart_dag = TriggerDagRunOperator(
        task_id="restart_entire_dag",
        trigger_dag_id="daily_prod_etl_medallion",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    reset_retry_counter = PythonOperator(
        task_id="reset_dag_retry_counter",
        python_callable=reset_dag_retry_counter,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Extract tasks run in parallel, then feed into create_tables (spark_raw_tasks[0])
    extract_tasks >> spark_raw_tasks[0]

    spark_raw_tasks[0].trigger_rule = TriggerRule.NONE_FAILED

    # create_tables completes first, then all load tasks run in parallel
    spark_raw_tasks[0] >> spark_raw_tasks[1:] >> \
        dbt_deps_task >> \
        dbt_seed_task >> \
        dbt_canonical_run_task >> \
        dbt_canonical_validation_task >> \
        dbt_analytics_run_task >> \
        dbt_analytics_validation_task

    # Success path → promote to prod → reset retry counter
    dbt_analytics_validation_task >> dbt_promote_canonical_task >> dbt_promote_analytics_task >> reset_retry_counter

    # Failure path → wait → retry DAG
    dbt_analytics_validation_task >> wait_30_minutes
    wait_30_minutes >> check_retry_limit >> restart_dag
