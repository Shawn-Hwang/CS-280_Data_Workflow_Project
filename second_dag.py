from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def first_task_function():
    # log.info("Welcome to CS 280! This is your first task")
    # name = "Enter your name here"
    log.info(f"Step 1")
    return

def second_task_function():
    # log.info("This is your second task")
    # major = "Enter your name here"
    log.info(f"Step 2")
    return

def third_task_function():
    # log.info("This is your third task")
    # hometown = "Enter your hometown here"
    log.info(f"Step 333")
    return

with DAG(
    dag_id="my_second_cs280_dag",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="step_00")
    first_task = PythonOperator(task_id="step_01", python_callable=first_task_function)
    second_task = PythonOperator(task_id="step_02", python_callable=second_task_function)
    third_task = PythonOperator(task_id="step_03", python_callable=third_task_function)
    end_task = DummyOperator(task_id="step_04")

start_task >> first_task >> third_task >> second_task >> end_task