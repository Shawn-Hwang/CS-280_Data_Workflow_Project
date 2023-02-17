from airflow import DAG
import requests
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import pandas as pd
import numpy as np



with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    # schedule_interval=None,
    start_date=pendulum.datetime(2023, 2, 19, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = DummyOperator(task_id="load_data_task")
    call_api_task = DummyOperator(task_id="call_api_task")
    transform_data_task = DummyOperator(task_id="transform_data_task")
    write_data_task = DummyOperator(task_id="write_data_task")

    load_data_task >> call_api_task >> transform_data_task >> write_data_task