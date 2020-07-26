import sys
import json
import pandas as pd
from tqdm import tqdm
import time
import datetime
import schedule

from apiclient.discovery import build

from google.cloud import bigquery
from google.oauth2 import service_account

secret_dir = '../secret'

with open(f'../config/bqtable_cfg.json', 'r') as f:
    bqtable_cfg = json.load(f)

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

sys.path.append(secret_dir)
import devkey
DEVELOPER_KEY = devkey.api2
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# connection build
YT = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)


def read_ytchannels_from_bq():

    table_name = 'idol_datalake_ytchannel'
    project_name = bqtable_cfg[table_name]['project_name']
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    QUERY = (
        f'SELECT name, channel_id FROM `{project_name}.{dataset_name}.{table_name}` '
    )
    query_job = client.query(QUERY)
    rows = query_job.result()
    df_channels = pd.DataFrame([[row.name, row.channel_id] for row in rows], columns=['name', 'channel_id'])
    return df_channels


def get_channel_stats(yt, channel_id):
    feed = YT.channels().list(
        part='id,statistics',
        id=channel_id,
    ).execute()
    stats = feed.get("items", [])[0]['statistics']
    return stats


def insert_idol_youtube_stats():

    df_channels = read_ytchannels_from_bq()
    channel_id_list = df_channels['channel_id'].values.tolist()
    now = datetime.datetime.now()
    rows = []
    for channel_id in tqdm(channel_id_list):
        try:
            stats = get_channel_stats(YT, channel_id)
            rows.append([now, channel_id, stats['videoCount'], stats['viewCount'], stats['subscriberCount']])
        except:
            continue
    df = pd.DataFrame(rows, columns=['timestamp', 'channel_id', 'videoCount', 'viewCount', 'subscriberCount'])

    # add df to bq
    table_name = 'idol_datalake_ytchannel_dailystats'
    dataset_name = bqtable_cfg[table_name]['dataset_name']

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    table_ref = client.dataset(dataset_name).table(table_name)
    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()
    print(f'Complete job!')
    return


if __name__ == "__main__":

    schedule.every(1).hours.do(insert_idol_youtube_stats)
    while True:
        schedule.run_pending()
        time.sleep(10)
