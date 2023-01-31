from airflow import DAG
import requests
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance
from google.cloud import storage
from gcsfs import GCSFileSystem
from databox import Client
import pandas as pd

# Get Bearer token, user list, and tweet list
bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
user_list = Variable.get("TWITTER_USER_IDS",[],deserialize_json=True)
tweet_list = Variable.get("TWITTER_TWEET_IDS",[],deserialize_json=True)

print(bearer_token)
print(user_list)
print(tweet_list)