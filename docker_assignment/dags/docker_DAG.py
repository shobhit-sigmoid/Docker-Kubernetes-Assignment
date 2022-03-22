from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from add_entries import insert_data

default_args = {
    "owner": "Shobhit",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 22),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG("db_insert", default_args=default_args, schedule_interval="0 6 * * *")
t1 = PythonOperator(task_id='Inserting_data_to_DB_table', python_callable=insert_data, dag=dag)

t1