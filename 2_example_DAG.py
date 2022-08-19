from airflow import DAG

from datetime import datetime
from airflow.operators.bash_operator import BashOperator


with DAG(dag_id = '2_example_DAG', start_date = datetime(2022, 8, 5), schedule_interval = '10 * * * *') as dag:
    t1 = BashOperator(
        task_id = 'print_date',
        bash_command = 'date'
    )
    t2 = BashOperator(
        task_id = 'print_hello',
        bash_command = 'echo "hello"'
    )
    t1 >> t2