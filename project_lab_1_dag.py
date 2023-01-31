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

def get_twitter_api_data_func(ti: TaskInstance, **kwargs):

    # Get Bearer token, user list, and tweet list
    # bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
    # user_list = Variable.get("TWITTER_USER_IDS",[],deserialize_json=True)
    # tweet_list = Variable.get("TWITTER_TWEET_IDS",[],deserialize_json=True)
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAPgnlgEAAAAAfbguIgh3wwfZUrWkeizpD%2BXKtXA%3DIMVi3vlwxJbTNYLGMbUzW5BwkbTdVO7ZjxLHmxjrio0bwzqXRI'
    user_list = [44196397,62513246,11348282,5162861,1349149096909668363]
    tweet_list = [1617886106921611270,1617975448641892352,1616850613697921025,1620119674913447940,1620110094997266432]

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

def push_data_to_databox_func():

    user_token = "k7oy27jcfqsui5dh1nrqe"

    # Intialize the databox client
    dbox = Client(user_token)

    # Retrieve info from google cloud bucket
    fs = GCSFileSystem(project="s-h-apache-airflow-cs280")
    with fs.open('gs://s-h-apache-airflow-cs280/data/project_lab_1_users.csv', 'rb') as f:
        user_df = pd.read_csv(f)
    with fs.open('gs://s-h-apache-airflow-cs280/data/project_lab_1_tweets.csv', 'rb') as f:
        tweet_df = pd.read_csv(f)
    
    # Create a metric for each user and push to databox
    for user in user_df['name'].values:
        metric_name = user.replace(' ', '_').lower()
        target_row = user_df.loc[user_df['name'] == f'{user}']
        followers_count = target_row['followers_count'].values[0]
        following_count = target_row['following_count'].values[0]
        tweet_count = target_row['tweet_count'].values[0]
        listed_count = target_row['listed_count'].values[0]

        dbox.push(f"{metric_name}_followers_count", followers_count)
        dbox.push(f"{metric_name}_following_count", following_count)
        dbox.push(f"{metric_name}_tweet_count", tweet_count)
        dbox.push(f"{metric_name}_listed_count", listed_count)
    
    # Create a metric for each tweet and push to databox
    for tweet in tweet_df['tweet_id'].values:
        target_row = tweet_df.loc[tweet_df['tweet_id'] == f'{tweet}']
        reply_count = target_row['reply_count'].values[0]
        like_count = target_row['like_count'].values[0]
        impression_count = target_row['impression_count'].values[0]
        retweet_count = target_row['retweet_count'].values[0]

        dbox.push(f"{tweet}_reply_count", reply_count)
        dbox.push(f"{tweet}_like_count", like_count)
        dbox.push(f"{tweet}_impression_count", impression_count)
        dbox.push(f"{tweet}_retweet_count", retweet_count)

    return

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    # schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    first_task = PythonOperator(task_id="get_twitter_api_data_task",
                                python_callable=get_twitter_api_data_func,
                                provide_context=True)
    second_task = PythonOperator(task_id="transform_twitter_api_data_task",
                                python_callable=transform_twitter_api_data_func,
                                provide_context=True)
    third_task = PythonOperator(task_id="push_data_to_databox_task",
                                python_callable=push_data_to_databox_func)

    first_task >> second_task >> third_task