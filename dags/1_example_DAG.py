import airflow 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(dag_id = '1_example_DAG', 
        schedule_interval='10 * * * *', 
        start_date=datetime(2022,8,1)) as dag:

    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4')

    t1 >> t2 >> t3 >> t4