from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

# specs for the operator
default_args = {
    "owner": "coder2j",
    "retries": 5,
    "retry_delay": timedelta(minutes = 2)
     
}
with DAG(
    dag_id = "our_first_dag",
    description = "This is our first dag that we write",
    default_args = default_args,
    start_date = datetime(2024,12,9,23),
    schedule_interval = "@daily"
) as dag:
    task1 = BashOperator(
    task_id = "first_task",
    bash_command = "echo hello world, this is the first task!"
    )
    task1