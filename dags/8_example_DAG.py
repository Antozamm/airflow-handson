import os

from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

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

def func1(**kwargs):
    pprint(kwargs)

def func2():
    dt = datetime.now().day
    if (dt%2) == 0:
        return 'pari'
    else:
        return 'dispari'

default_args ={
    'owner': 'az',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
    'on_success_callback': _task_success,
    'on_failure_callback': _task_fail
}


with DAG(dag_id='8_example_DAG', \
    start_date=datetime(day=8, month=8, year=2022), \
    schedule_interval=timedelta(minutes=10), \
    catchup=False, \
    default_args=default_args) as dag:
    
    t1 = PythonOperator( task_id='write_file', \
            python_callable=write_file, \
            op_kwargs={"filename2":'newtest.txt'})

    t2 = FileSensor(task_id='check_file', filepath='.', fs_conn_id='fs_default')

    t3 = PythonOperator( task_id='leave_a_message', python_callable=func1)

    t4 = BranchPythonOperator(task_id='test_branching', python_callable=func2)

    t5 = BashOperator(task_id='pari', bash_command='echo "Bash branch pari"')
    t6 = BashOperator(task_id='dispari', bash_command='echo "Bash branch dispari"')

    

    

t1>>t2
t2>>t3
t3>>t4
t4>>[t5,t6]
