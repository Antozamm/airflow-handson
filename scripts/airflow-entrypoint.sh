#!/usr/bin/env bash
airflow resetdb
airflow db init
airflow upgradedb
airflow users create -r Admin -u admin -e jyotisachdeva57@gmail.com -f jyoti -l sachdeva -p admin
airflow scheduler &
airflow webserver