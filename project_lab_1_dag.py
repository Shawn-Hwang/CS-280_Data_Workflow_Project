from airflow import DAG
import requests
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance
from google.cloud import storage
import pandas as pd

def get_twitter_api_data_func(ti: TaskInstance, **kwargs):

    # Get Bearer token, user list, and tweet list
    bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
    user_list = Variable.get("TWITTER_USER_IDS",[],deserialize_json=True)
    tweet_list = Variable.get("TWITTER_TWEET_IDS",[],deserialize_json=True)

    # Create the authentication header
    authentication_header = {"Authorization": f"Bearer {bearer_token}"}

    # Initialize result lists
    user_results = []
    tweet_results = []

    # Run request for every single user-id
    for user_id in user_list:
        api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
        request = requests.get(api_url, headers=authentication_header)

        # Check status code
        if request.status_code != 200:
            log.info(f"Failed to get user: {user_id}.")
        else:
            result = request.json()
            log.info(result)
            user_results.append(result)

    # Run request for every single tweet-id
    for tweet_id in tweet_list:
        api_url = f'https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=author_id,public_metrics,text'

        # Check status code
        if request.status_code != 200:
            log.info(f"Failed to get tweet: {user_id}.")
        else:
            result = request.json()
            log.info(result)
            tweet_results.append(result)

    log.info(f"Finished requesting.")

    # Send result lists to next task
    ti.xcom_push("user_results", user_results)
    ti.xcom_push("tweet_results", tweet_results)
    return

def create_user_df(user_results):
    user_ids = [u['data']['id'] for u in user_results]
    usernames = [u['data']['username'] for u in user_results]
    names = [u['data']['name'] for u in user_results]
    followers_count = [u['data']['public_metrics']['followers_count'] for u in user_results]
    following_count = [u['data']['public_metrics']['following_count'] for u in user_results]
    tweet_count = [u['data']['public_metrics']['tweet_count'] for u in user_results]
    listed_count = [u['data']['public_metrics']['listed_count'] for u in user_results]

    user_df = pd.DataFrame(data={'user_id': user_ids,
                                'username': usernames,
                                'name': names,
                                'followers_count': followers_count,
                                'following_count': following_count,
                                'tweet_count': tweet_count,
                                'listed_count': listed_count})
    
    return user_df

def create_tweet_df(tweet_results):
    tweet_ids = [t['data']['id'] for t in tweet_results]
    texts = [t['data']['text'] for t in tweet_results]
    retweet_count = [t['data']['public_metrics']['retweet_count'] for t in tweet_results]
    reply_count = [t['data']['public_metrics']['reply_count'] for t in tweet_results]
    like_count = [t['data']['public_metrics']['like_count'] for t in tweet_results]
    quote_count = [t['data']['public_metrics']['quote_count'] for t in tweet_results]
    impression_count = [t['data']['public_metrics']['impression_count'] for t in tweet_results]

    tweet_df = pd.DataFrame(data={'tweet_id': tweet_ids,
                                'text': texts,
                                'retweet_count': retweet_count,
                                'reply_count': reply_count,
                                'like_count': like_count,
                                'quote_count': quote_count,
                                'impression_count': impression_count})

    return tweet_df

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):

    # Get user results and tweet results from last task
    user_results = ti.xcom_pull(key="user_results", task_ids="get_twitter_api_data_task")
    tweet_results = ti.xcom_pull(key="tweet_results", task_ids="get_twitter_api_data_task")

    # Create the user df
    user_df = create_user_df(user_results)

    # Create the tweet df
    tweet_df = create_tweet_df(tweet_results)
    
    # Upload CSVs to Bucket
    client = storage.Client()
    bucket = client.get_bucket("s-h-apache-airflow-cs280")
    bucket.blob("data/project_lab_1_users.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/project_lab_1_tweets.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")
    return

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    first_task = PythonOperator(task_id="get_twitter_api_data_task",
                                python_callable=get_twitter_api_data_func,
                                provide_context=True)
    second_task = PythonOperator(task_id="transform_twitter_api_data_task",
                                python_callable=transform_twitter_api_data_func,
                                provide_context=True)

first_task >> second_task