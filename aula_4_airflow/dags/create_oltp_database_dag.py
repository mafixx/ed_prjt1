from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os

# Caminho do AIRFLOW_HOME
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

# Caminho onde estão os arquivos SQL
TEMPLATE_PATH = os.path.join(AIRFLOW_HOME, "custom_packages")

DEFAULT_ARGS = {
    "start_date": datetime(2025, 10, 4),
    "retries": 2,
}

@dag(
    dag_id="create_oltp_database_dag",
    description="DAG para estruturar e popular um ambiente OLTP no PostgreSQL.",
    schedule=None,
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    template_searchpath=TEMPLATE_PATH,  # IMPORTANTE: sem lista
)
def oltp_medallion_pipeline():

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    create_oltp_structure = SQLExecuteQueryOperator(
        task_id="create_oltp_structure",
        conn_id="postgres_oltp_conn",
        sql="create_table.sql",
        autocommit=False, # Persiste no banco após todo o arquivo ser executado no banco
    )

    insert_oltp_data = SQLExecuteQueryOperator(
        task_id="insert_oltp_data",
        conn_id="postgres_oltp_conn",
        sql="insert_into.sql",
        autocommit=False, # Persiste no banco após todo o arquivo ser executado no banco
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

    start_pipeline >> create_oltp_structure >> insert_oltp_data >> end_pipeline


dag_instance = oltp_medallion_pipeline()