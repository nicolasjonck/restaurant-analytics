#!/usr/bin/env bash

# Create an empty postgres db inside "postgres" container (airflow db init)
# Then create an empty db schema with airflow metadata tables
airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow

airflow webserver
