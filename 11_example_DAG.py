from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def download(file):
    print('Downloading {}'.format(file))

def _python_callable1():
    print("AIRFLOW_CTX_DAG_ID")

def download_file(file: str):
        download(file)


with DAG('11_example_DAG', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:


    t0 = PythonOperator(task_id='t0', python_callable=download_file, op_kwargs={'file': 'file_a'})


    t1 = BashOperator(task_id='start', bash_command='echo "start"')

    for i in range(1,4):
        t2 = PythonOperator(task_id='task_t{}'.format(i), python_callable=_python_callable1 )

        t1 >> t2

    t0 >> t1