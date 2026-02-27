from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("Airflow está funcionando corretamente!")


with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste"],
) as dag:

    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )