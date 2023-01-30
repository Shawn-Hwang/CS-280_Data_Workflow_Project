from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance

def get_twitter_api_data_task(ti: TaskInstance, **kwargs):
    
    pass


with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    first_task = PythonOperator(task_id="scrape_twitter_users_and_tweets",
                                python_callable=get_twitter_api_data_task,
                                provide_context=True)

first_task