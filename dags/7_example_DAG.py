import os

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta

from airflow.utils.dates import days_ago


# airflow connections add 'fs_default' --conn-type='fs'

def write_file(filename2):
    
    if os.path.exists(filename2):
        os.remove(filename2)
        print(f'Removing file {filename2}')

    with open(filename2, 'w') as f:
        f.write('prova testo\n')

def _task_success(context):
    print('Task successo')

def _task_fail(context):
    print('Task fail')

default_args ={
    'owner': 'az',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': _task_success,
    'on_failure_callback': _task_fail
}


with DAG(dag_id='7_example_DAG', \
    start_date=datetime(day=8, month=8, year=2022), \
    schedule_interval=timedelta(minutes=10), \
    catchup=False, \
    default_args=default_args) as dag:
    
    t1 = PythonOperator( task_id='write_file', \
            python_callable=write_file, \
            op_kwargs={"filename2":'newtest.txt'})

    t2 = FileSensor(task_id='check_file', filepath='.', fs_conn_id='fs_default')

t1>>t2