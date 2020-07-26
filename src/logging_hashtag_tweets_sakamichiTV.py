
import json
import tweepy
import pandas as pd

from apiclient.discovery import build

from google.cloud import bigquery
from google.oauth2 import service_account

secret_dir = '../secret'

with open(f'../config/bqtable_cfg.json', 'r') as f:
    bqtable_cfg = json.load(f)
with open(f'../config/hashtag_query_cfg.json', 'r') as f:
    hashtag_query_cfg = json.load(f)

# gcp setting
key_path = f'{secret_dir}/tkhr-free-myaun.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

table_name = 'idol_datalake_hashtagtweets_sakamichiTV'


class StdOutListener(tweepy.StreamListener):
    def __init__(self, insert_div=1000):
        self.stream_num = 0
        self.save_stream_num = 0
        self.insert_div = insert_div
        self.rows = []

    def on_data(self, data):

        self.stream_num += 1

        tweet = json.loads(data)

        # retweet fileter
        if not tweet['retweeted'] and 'RT @' not in tweet['text']:
            if self.save_stream_num != 0 and self.save_stream_num % self.insert_div == 0:
                print(self.stream_num, self.save_stream_num)
                self.insert_hashtag_tweets(self.rows)
                self.rows = []

            hashtags = '\t'.join([
                i["text"] for i in tweet["entities"].get("hashtags", [])
            ])

            urls = '\t'.join([
                i["url"] for i in tweet["entities"].get("urls", [])
            ])

            self.rows.append([
                tweet["created_at"], tweet["id_str"], tweet["text"],
                tweet["user"]["id_str"], tweet["user"]["followers_count"], tweet["user"]["friends_count"],
                # tweet["geo"], tweet["place"], tweet["coordinates"], tweet["lang"],
                hashtags, urls
            ])

            self.save_stream_num += 1
            return True
        else:
            # do not save
            return True

    def on_error(self, status):
        print(status)

    def insert_hashtag_tweets(self, rows):

        col_names = [
            "created_at", "id", "text",
            "user_id", "user_followers_count", "user_friends_count",
            # "geo", "place", "coordinates", "lang",
            "hashtags", "urls"
        ]
        df = pd.DataFrame(rows, columns=col_names)

        # add df to bq
        dataset_name = bqtable_cfg[table_name]['dataset_name']

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        table_ref = client.dataset(dataset_name).table(table_name)
        load_job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        load_job.result()
        print(f'Complete BQ job!')

        return


def start_stream(auth, sol, queries):
    while True:
        try:
            print('Start logging ...')
            stream = tweepy.Stream(auth, sol)
            stream.filter(track=queries)
        except Exception as e:
            print('Error', e)
            continue


def main():

    queries = hashtag_query_cfg[table_name]
    with open('../secret/twitterapi.json', 'r') as f:
        key_dic = json.load(f)

    key_id = "10"

    sol = StdOutListener(insert_div=1000)
    auth = tweepy.OAuthHandler(key_dic[key_id]["app_key"], key_dic[key_id]["app_secret"])
    auth.set_access_token(key_dic[key_id]["oauth_token"], key_dic[key_id]["oauth_token_secret"])

    print('target query:', queries)
    start_stream(auth, sol, queries)


if __name__ == "__main__":
    main()
