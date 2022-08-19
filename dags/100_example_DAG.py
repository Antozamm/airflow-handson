
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from airflow.decorators import dag, task

from datetime import timedelta, datetime

from airflow.models.baseoperator import chain

from airflow.utils.trigger_rule import TriggerRule

def _task_fail(**kwargs):
    print('TASK FAILED')
    raise Exception('Task failed')

def _task_success(**kwargs):
    print('TASK SUCCESS')
    pass

default_args = {
    'owner': 'az',
    'depends_on_past': False,
    'catchup': False,
    # 'on_failure_callback': _task_fail,
    # 'on_success_callback': _task_success,
}

with DAG(dag_id='100_example_DAG', start_date=days_ago(1), schedule_interval=timedelta(days=1)) as dag:

    start = EmptyOperator(task_id='start')

    with TaskGroup(group_id='TaskGroup1') as tg1:

        t1 = BashOperator(task_id='t1', bash_command='echo "t1"; exit 0')

        t2 = BashOperator(task_id='t2', bash_command='echo "t2"; exit 1')
 


    end1 = EmptyOperator(task_id='end_one_success', trigger_rule=TriggerRule.ONE_SUCCESS)
    end2 = EmptyOperator(task_id='end_one_fail', trigger_rule=TriggerRule.ONE_FAILED)
    end3 = EmptyOperator(task_id='end_all_success', trigger_rule=TriggerRule.ALL_SUCCESS)


    # start >> tg1
    # tg1   >> end

    chain(start, tg1, [end1, end2, end3])
