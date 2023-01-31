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
bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
user_list = Variable.get("TWITTER_USER_IDS")
tweet_list = Variable.get("TWITTER_TWEET_IDS")

print(bearer_token)
print(user_list)
print(tweet_list)

DATABOX_TOKEN 	k7oy27jcfqsui5dh1nrqe
TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET
TWITTER_API_KEY
TWITTER_API_SECRET
TWITTER_BEARER_TOKEN 'AAAAAAAAAAAAAAAAAAAAAPgnlgEAAAAAfbguIgh3wwfZUrWkeizpD%2BXKtXA%3DIMVi3vlwxJbTNYLGMbUzW5BwkbTdVO7ZjxLHmxjrio0bwzqXRI'
TWITTER_TWEET_IDS [1617886106921611270,1617975448641892352,1616850613697921025,1620119674913447940,1620110094997266432]
TWITTER_USER_IDS [44196397,62513246,11348282,5162861,1349149096909668363]