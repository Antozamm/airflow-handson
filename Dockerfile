FROM apache/airflow
USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags

USER airflow
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org boto3

USER ${AIRFLOW_UID}