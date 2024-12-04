from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='first_dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2024,month=11,day=30),
    catchup=False
) as dag:
    task_get_datetime=BashOperator(
        task_id='get_datetime',
        bash_command='date'
    )