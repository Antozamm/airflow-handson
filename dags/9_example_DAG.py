from airflow import DAG
from airflow.decorators import task
from datetime import datetime

def download(file):
    print('Downloading {}'.format(file))

with DAG('9_example_DAG', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    @task
    def download_file(file: str):
        download(file)

    files = download_file.expand(file=["file_a", "file_b", "file_c"])