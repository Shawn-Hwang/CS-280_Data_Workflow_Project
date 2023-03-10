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
import pandas as pd
import numpy as np
from models.config import Session #You would import this from your config file
from models.users import User
from models.tweets import Tweet
from models.user_timeseries import UserTimeSeries
from models.tweet_timeseries import TweetTimeSeries
from datetime import datetime

def load_data_task_func(ti: TaskInstance, **kwargs):
    session = Session()

    # Get all user_ids
    users_lst = session.query(User).all()
    user_ids = [u.user_id for u in users_lst]

    # Get all tweet_ids
    tweets_lst = session.query(Tweet).all()
    tweet_ids = []
    if len(tweets_lst) > 0:
        tweet_ids = [(t.tweet_id, t.user_id) for t in tweets_lst]
    log.info(f"Pulled tweets: {len(tweet_ids)}.")

    # Pass user_ids and tweet_ids to the next task thru xcom
    ti.xcom_push("user_ids", user_ids)
    ti.xcom_push("tweet_ids", tweet_ids)
    return

def call_api_task_func(ti: TaskInstance, **kwargs):
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAADyxlQEAAAAAOVo8JJxuAA6l1RI%2BPF04u9pzdL8%3Dm2rimTHbpVx5HRm22oAAvdDHvomOKFgmeeg8JwTYtEVTVJK8Cf'
    # Create the authentication header
    authentication_header = {"Authorization": f"Bearer {bearer_token}"}

    # Get user results and tweet results from last task
    user_ids = ti.xcom_pull(key="user_ids", task_ids="load_data_task")
    tweet_ids = ti.xcom_pull(key="tweet_ids", task_ids="load_data_task")

    # Initialize result lists
    user_results = []
    tweet_results = []

    # Run request for every single user-id
    for user_id in user_ids:
        api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics"
        request = requests.get(api_url, headers=authentication_header)

        # Check status code
        if request.status_code != 200:
            log.info(f"Failed to get the info of user: {user_id}.")
        else:
            result = request.json()
            log.info(result)
            user_results.append(result)

    # Pass all information to the next task
    ti.xcom_push("user_results", user_results)



    # Run request for every single tweet-id
    if len(tweet_ids) > 0:
        for tweet_id_pair in tweet_ids:
            tweet_id = tweet_id_pair[0]
            api_url = f'https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=author_id,public_metrics,text'
            request = requests.get(api_url, headers=authentication_header)

            # Check status code
            if request.status_code != 200:
                log.info(f"Failed to get the info of tweet: {tweet_id}.")
            else:
                result = request.json()
                log.info(result)
                tweet_results.append(result)

    # Pass all information to the next task
    ti.xcom_push("tweet_results", tweet_results)



    # Retrieve the last 5 tweets and their statistics for every user
    last_tweets_results = []
    for user_id in user_ids:
        api_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={user_id}&count=5'
        request = requests.get(api_url, headers=authentication_header)
        # Check status code
        if request.status_code != 200:
            log.info(f"Failed to retrieve tweets of user: {user_id}.")
        else:
            result = request.json()
            last_tweets_results.append(result)
            log.info(result[0])

    # Pass all information to the next task
    ti.xcom_push("last_tweets_results", last_tweets_results)

    return


def transform_data_task_func(ti: TaskInstance, **kwargs):
    curr_date = datetime.now()

    # Get results from last task
    user_results = ti.xcom_pull(key="user_results", task_ids="call_api_task")
    tweet_results = ti.xcom_pull(key="tweet_results", task_ids="call_api_task")
    last_tweets_results = ti.xcom_pull(key="last_tweets_results", task_ids="call_api_task")

    # Create user dataframe
    # Get every user's updated statistic
    user_ids = [u['data']['id'] for u in user_results]
    followers_count = [u['data']['public_metrics']['followers_count'] for u in user_results]
    following_count = [u['data']['public_metrics']['following_count'] for u in user_results]
    tweet_count = [u['data']['public_metrics']['tweet_count'] for u in user_results]
    listed_count = [u['data']['public_metrics']['listed_count'] for u in user_results]
    date = [curr_date for _ in range(len(user_ids))]

    # Make dataframe
    user_df = pd.DataFrame(data={'user_id': user_ids,
                                'followers_count': followers_count,
                                'following_count': following_count,
                                'tweet_count': tweet_count,
                                'listed_count': listed_count,
                                'date': date})
    
    # Create tweet dataframe
    # Get every tweet's updated and static statistics
    # This includes: tweet_id, user_id, text, created_at, retweet_count, favorite/like_count, date, newly_retrieved(Boolean)

    # Get info from all current tweets
    curr_tweet_ids = [t['data']['id'] for t in tweet_results]
    user_ids = [t['data']['author_id'] for t in tweet_results]
    texts = [t['data']['text'] for t in tweet_results]
    created_at = ['Empty' for _ in tweet_results]
    retweet_count = [t['data']['public_metrics']['retweet_count'] for t in tweet_results]
    favorite_count = [t['data']['public_metrics']['like_count'] for t in tweet_results]
    date = [curr_date for _ in range(len(curr_tweet_ids))]
    newly_retrieved = [False for _ in tweet_results]

    # Make df for all current tweets
    curr_tweet_df = pd.DataFrame(data={'tweet_id': curr_tweet_ids,
                                'user_id': user_ids,
                                'text': texts,
                                'created_at': created_at,
                                'retweet_count': retweet_count,
                                'favorite_count': favorite_count,
                                'date': date,
                                'newly_retrieved': newly_retrieved})
    
    # Get info from all newly retrieved tweet
    new_tweet_ids = []
    user_ids = []
    texts = []
    created_at = []
    retweet_count = []
    favorite_count = []
    newly_retrieved = []

    for each_user in last_tweets_results:
        for tweet in each_user:
            # Check if this is a new tweet
            tweet_id = tweet['id_str']
            if tweet_id not in curr_tweet_ids:
                new_tweet_ids.append(tweet_id)
                user_ids.append(tweet['user']['id'])
                texts.append(tweet['text'])
                created_at.append(tweet['created_at'])
                retweet_count.append(tweet['retweet_count'])
                favorite_count.append(tweet['favorite_count'])
                newly_retrieved.append(True)
    date = [curr_date for _ in range(len(new_tweet_ids))]

    log.info(f'Length of last_tweets_results: {len(last_tweets_results)}')
    log.info(f'Length of new_tweet_ids: {len(new_tweet_ids)}')
    log.info(f'Length of user_ids: {len(user_ids)}')
    log.info(f'Length of texts: {len(texts)}')
    log.info(f'Length of created_at: {len(created_at)}')
    log.info(f'Length of retweet_count: {len(retweet_count)}')
    log.info(f'Length of favorite_count: {len(favorite_count)}')
    log.info(f'Length of newly_retrieved: {len(newly_retrieved)}')
    log.info(f'Length of date: {len(date)}')
    

    # Make df for newly retrieved tweets
    new_tweet_df = pd.DataFrame(data={'tweet_id': new_tweet_ids,
                                'user_id': user_ids,
                                'text': texts,
                                'created_at': created_at,
                                'retweet_count': retweet_count,
                                'favorite_count': favorite_count,
                                'date': date,
                                'newly_retrieved': newly_retrieved})
    
    # Concatenate two tweet dfs into one
    tweet_df = pd.concat([curr_tweet_df, new_tweet_df])
    

    # Send CSVs to Google Bucket
    client = storage.Client()
    bucket = client.get_bucket("s-h-apache-airflow-cs280")
    bucket.blob(f"data/project_lab_2_users.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob(f"data/project_lab_2_tweets.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")

    # Pass date to the next task
    ti.xcom_push("date", curr_date.strftime("%Y_%M_%D_%H:%M:%S"))


def write_data_task_func(ti: TaskInstance, **kwargs):
    # # Get date from last task
    # date = ti.xcom_pull(key="date", task_ids="transform_data_task")

    # Retrieve info from google cloud bucket
    fs = GCSFileSystem(project="shawn-huang-cs-280-375620")
    with fs.open('s-h-apache-airflow-cs280/data/project_lab_2_users.csv', 'rb') as f:
        user_df = pd.read_csv(f)
    with fs.open('s-h-apache-airflow-cs280/data/project_lab_2_tweets.csv', 'rb') as f:
        tweet_df = pd.read_csv(f)

    session = Session()
    date = user_df['date'].values[0]

    log.info(user_df.to_dict())
    # Add all users' statistics to user_timeseries table
    for user_id in user_df['user_id'].values:
        target_row = user_df.loc[user_df['user_id'] == user_id]
        log.info(f'Length of target tow: {len(target_row)}')
        user_timeseries = UserTimeSeries(
            user_id=str(user_id),
            followers_count = int(target_row['followers_count'].values[0]),
            following_count = int(target_row['following_count'].values[0]),
            tweet_count = int(target_row['tweet_count'].values[0]),
            listed_count = int(target_row['listed_count'].values[0]),
            date = date
          )
        session.add(user_timeseries) 
    
    # Add new tweets to the tweet table
    new_tweets = tweet_df.loc[tweet_df['newly_retrieved'] == 1]
    for tweet_id in new_tweets['tweet_id'].values:
        target_row = new_tweets.loc[new_tweets['tweet_id'] == tweet_id]
        new_tweet = Tweet(
            tweet_id = str(tweet_id),
            user_id = int(target_row['user_id'].values[0]),
            text = str(target_row['text'].values[0]),
            created_at = str(target_row['created_at'].values[0])
        )
        session.add(new_tweet) 

    # Update tweet_timeseries table
    for tweet_id in tweet_df['tweet_id'].values:
        target_row = tweet_df.loc[tweet_df['tweet_id'] == tweet_id]
        tweet_timeseries = TweetTimeSeries(
            tweet_id = str(tweet_id),
            retweet_count = int(target_row['retweet_count'].values[0]),
            favorite_count = int(target_row['favorite_count'].values[0]),
            date = date
        )
        session.add(tweet_timeseries)



    # Commit your changes to the database
    session.commit()

    # Close the connection
    session.close()


    return



with DAG(
    dag_id="project_lab_2",
    schedule_interval="0 9 * * *",
    # schedule_interval=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = PythonOperator(task_id="load_data_task",
                                    python_callable=load_data_task_func,
                                    provide_context=True)
    call_api_task = PythonOperator(task_id="call_api_task",
                                   python_callable=call_api_task_func,
                                   provide_context=True)
    transform_data_task = PythonOperator(task_id="transform_data_task",
                                         python_callable=transform_data_task_func,
                                         provide_context=True)
    write_data_task = PythonOperator(task_id="write_data_task",
                                     python_callable=write_data_task_func,
                                     provide_context=True)

    load_data_task >> call_api_task >> transform_data_task >> write_data_task